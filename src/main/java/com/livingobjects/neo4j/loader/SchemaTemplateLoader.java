package com.livingobjects.neo4j.loader;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.CacheNodeFactory;
import com.livingobjects.neo4j.helper.CacheNodeLoader;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.helper.StringTemplate;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.model.exception.SchemaTemplateException;
import com.livingobjects.neo4j.model.header.HeaderElement;
import com.livingobjects.neo4j.model.schema.Property;
import com.livingobjects.neo4j.model.schema.SchemaTemplate;
import com.livingobjects.neo4j.model.schema.SchemaVersion;
import com.livingobjects.neo4j.model.schema.relationships.Relationships;
import com.livingobjects.neo4j.model.schema.xml.XMLSchemaTemplateHandler;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXSource;
import javax.xml.validation.SchemaFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class SchemaTemplateLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaTemplateLoader.class);

    private static final DynamicRelationshipType APPLIED_TO_LINK = DynamicRelationshipType.withName("AppliedTo");
    public static final int SCHEMA_TEMPLATE_APPLIED_IN_TRANSACTION = 400;

    private final GraphDatabaseService graphDB;

    private final Map<NodeType, CacheNodeFactory> nodeFactories = Maps.newHashMap();

    private final CacheNodeLoader cacheNodeLoader;

    public SchemaTemplateLoader(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
        this.cacheNodeLoader = new CacheNodeLoader(graphDB);
    }

    public int loadAndApplyTemplate(InputStream csv, InputStream xmlTemplate) throws IOException, IllegalStateException {
        long time = System.currentTimeMillis();
        CSVReader reader = new CSVReader(new InputStreamReader(csv));
        ImmutableMap<String, Integer> header = readCsvHeader(reader);

        SchemaTemplate template = parseTemplate(xmlTemplate);

        int updated = 0;
        int alreadyUpdated = 0;
        int committed = 0;
        String[] line = reader.readNext();

        Transaction tx = graphDB.beginTx();
        try {
            while (line != null) {

                if (applyTemplate(template, header, line)) {
                    updated++;
                } else {
                    alreadyUpdated++;
                }

                if (updated > 0 && (updated % SCHEMA_TEMPLATE_APPLIED_IN_TRANSACTION) == 0) {
                    committed = commitTx(updated, committed, tx);
                    updated = 0;
                    tx = graphDB.beginTx();
                }

                line = reader.readNext();
            }
            if (updated > 0) {
                committed = commitTx(updated, committed, tx);
            }
        } catch (Throwable e) {
            tx.close();
            throw e;
        }
        LOGGER.info("{} topology schema(s) updated in {} ms. {} schema(s) already up to date.", committed, (System.currentTimeMillis() - time), alreadyUpdated);
        return committed;
    }

    private int commitTx(int applied, int committed, Transaction tx) {
        tx.success();
        tx.close();
        nodeFactories.clear();
        cacheNodeLoader.clear();
        committed += applied;
        LOGGER.info("{} schema committed", committed);
        return committed;
    }

    private boolean applyTemplate(SchemaTemplate template, ImmutableMap<String, Integer> header, String[] line) {
        UniqueEntity<Node> templateNode = createNode(template.templateNode, header, line);

        if (shouldApplyTemplate(template, templateNode)) {
            createAndLinkTemplateVersion(template, templateNode);

            Map<String, org.neo4j.graphdb.Node> identifiedNodes = Maps.newHashMap();
            Set<CreatedNode> nodeWithRelationships = Sets.newHashSet();
            for (com.livingobjects.neo4j.model.schema.Node node : template.nodes) {
                UniqueEntity<org.neo4j.graphdb.Node> entity = createNode(node, header, line);
                node.id.ifPresent(id -> {
                    Node previsousNode = identifiedNodes.put(id, entity.entity);
                    if (previsousNode != null && !previsousNode.equals(entity.entity)) {
                        throw new IllegalStateException("Two diffenrnt nodes referenced with id = '" + id + "' in template XML file.");
                    }
                });
                if (!node.relationships.isEmpty()) {
                    nodeWithRelationships.add(new CreatedNode(node, entity.entity));
                }
            }

            for (CreatedNode node : nodeWithRelationships) {
                for (Relationships relationships : node.node.relationships) {
                    DynamicRelationshipType relationshipType = DynamicRelationshipType.withName(relationships.type);
                    Set<org.neo4j.graphdb.Node> alreadyLinkedNodes = Sets.newHashSet();
                    Iterable<Relationship> existingRelationship = node.createdNode.getRelationships(relationshipType, relationships.direction.neo4jDirection);
                    for (Relationship relationship : existingRelationship) {
                        if (relationships.replace) {
                            relationship.delete();
                        } else {
                            if (relationships.direction == Relationships.Direction.incoming) {
                                alreadyLinkedNodes.add(relationship.getStartNode());
                            } else {
                                alreadyLinkedNodes.add(relationship.getEndNode());
                            }
                        }
                    }
                    for (com.livingobjects.neo4j.model.schema.relationships.Relationship relationshipToCreate : relationships.relationships) {
                        relationshipToCreate.visit(new com.livingobjects.neo4j.model.schema.relationships.Relationship.Visitor() {
                            @Override
                            public void node(ImmutableSet<Property> properties, String relNode) {
                                org.neo4j.graphdb.Node otherSideNode = identifiedNodes.get(relNode);
                                if (otherSideNode == null) {
                                    throw new IllegalStateException("Unable to create relationship involving node '" + relNode + "' because this node is not found in template file.");
                                } else {
                                    createRelationship(properties, otherSideNode, relationships, alreadyLinkedNodes, node, relationshipType, header, line);
                                }
                            }

                            @Override
                            public void cypher(ImmutableSet<Property> properties, String cypher) {
                                ImmutableSet<org.neo4j.graphdb.Node> nodes = cacheNodeLoader.load(cypher);
                                for (org.neo4j.graphdb.Node nodeToLink : nodes) {
                                    createRelationship(properties, nodeToLink, relationships, alreadyLinkedNodes, node, relationshipType, header, line);
                                }
                            }
                        });
                    }
                }
            }

            return true;
        } else {
            return false;
        }
    }

    private void createRelationship(ImmutableSet<Property> properties, org.neo4j.graphdb.Node otherSideNode, Relationships relationships, Set<org.neo4j.graphdb.Node> alreadyLinkedNodes, CreatedNode node, DynamicRelationshipType relationshipType, ImmutableMap<String, Integer> header, String[] line) {
        if (relationships.replace || !alreadyLinkedNodes.contains(otherSideNode)) {
            Relationship createdRelationship;
            if (relationships.direction == Relationships.Direction.incoming) {
                createdRelationship = otherSideNode.createRelationshipTo(node.createdNode, relationshipType);
            } else {
                createdRelationship = node.createdNode.createRelationshipTo(otherSideNode, relationshipType);
            }
            setProperties(properties, header, line, createdRelationship);
        }
    }

    private void createAndLinkTemplateVersion(SchemaTemplate template, UniqueEntity<org.neo4j.graphdb.Node> templateNode) {
        CacheNodeFactory factory = nodeFactory(ImmutableList.of("Template"), ImmutableSet.of("name", "version"));
        UniqueEntity<org.neo4j.graphdb.Node> node = factory.getOrCreate(ImmutableSet.of(template.name, template.version.toString()));
        node.entity.createRelationshipTo(templateNode.entity, APPLIED_TO_LINK);
    }

    private boolean shouldApplyTemplate(SchemaTemplate template, UniqueEntity<org.neo4j.graphdb.Node> templateNode) {
        Iterable<Relationship> relationships = templateNode.entity.getRelationships(Direction.INCOMING, APPLIED_TO_LINK);
        for (Relationship relationship : relationships) {
            org.neo4j.graphdb.Node appliedTemplateNode = relationship.getStartNode();
            boolean isTemplateNode = false;
            for (Label label : appliedTemplateNode.getLabels()) {
                if (label.equals(DynamicLabel.label("Template"))) {
                    isTemplateNode = true;
                    break;
                }
            }
            if (isTemplateNode) {
                String templateName = appliedTemplateNode.getProperty("name", "").toString();
                if (templateName.equals(template.name)) {
                    String templateVersion = appliedTemplateNode.getProperty("version", "0").toString();
                    SchemaVersion schemaVersion = SchemaVersion.of(templateVersion);
                    if (schemaVersion.compareTo(template.version) < 0) {
                        relationship.delete();
                        if (!appliedTemplateNode.hasRelationship()) {
                            appliedTemplateNode.delete();
                        }
                        return true;
                    } else {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private UniqueEntity<org.neo4j.graphdb.Node> createNode(com.livingobjects.neo4j.model.schema.Node node, ImmutableMap<String, Integer> header, String[] line) {
        CacheNodeFactory factory = nodeFactory(node.labels, node.keys.keySet());

        ImmutableSet.Builder<String> transformedKeys = ImmutableSet.builder();
        for (Map.Entry<String, String> keyProperty : node.keys.entrySet()) {
            String value = keyProperty.getValue();
            String transformedValue = StringTemplate.template(value, header, line);
            transformedKeys.add(transformedValue);
        }

        UniqueEntity<org.neo4j.graphdb.Node> entity = factory.getOrCreate(transformedKeys.build());
        setProperties(node.properties, header, line, entity.entity);
        return entity;
    }

    private void setProperties(ImmutableSet<Property> properties, ImmutableMap<String, Integer> header, String[] line, PropertyContainer entity) {
        for (Property property : properties) {
            String value = StringTemplate.template(property.value, header, line);
            Object propertyValue = PropertyConverter.convert(value, property.type, property.isArray);
            if (propertyValue != null) {
                entity.setProperty(property.name, propertyValue);
            } else {
                entity.removeProperty(property.name);
            }
        }
    }

    private SchemaTemplate parseTemplate(InputStream xmlTemplate) throws SchemaTemplateException {
        try {
            SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
            SAXParserFactory factory = SAXParserFactory.newInstance();
            try (InputStream resourceAsStream = getClass().getResourceAsStream("/schema-template.xsd")) {
                javax.xml.validation.Schema xsdSchema = schemaFactory.newSchema(new SAXSource(new InputSource(resourceAsStream)));
                factory.setSchema(xsdSchema);
                SAXParser saxParser = factory.newSAXParser();
                XMLSchemaTemplateHandler handler = new XMLSchemaTemplateHandler();
                saxParser.parse(xmlTemplate, handler);
                return handler.getTemplate();
            }
        } catch (ParserConfigurationException | SAXException e) {
            throw new SchemaTemplateException("Unable to instanciate XML parser", e);
        } catch (IOException e) {
            throw new SchemaTemplateException("Unable to parse XML template", e);
        }
    }

    private ImmutableMap<String, Integer> readCsvHeader(CSVReader reader) throws IOException {
        ImmutableMap.Builder<String, Integer> headerBuilder = ImmutableMap.builder();
        String[] headerLine = reader.readNext();
        if (headerLine != null) {
            for (int index = 0; index < headerLine.length; index++) {
                String column = headerLine[index];
                HeaderElement headerElement = HeaderElement.of(column, index);
                headerBuilder.put(headerElement.elementName + '.' + headerElement.propertyName, index);
            }
        }
        return headerBuilder.build();
    }

    private CacheNodeFactory nodeFactory(ImmutableList<String> labels, ImmutableSet<String> keys) {
        NodeType nodeType = new NodeType(labels, keys);
        return nodeFactories.computeIfAbsent(nodeType, k -> CacheNodeFactory.of(graphDB, labels, keys));
    }

    private final class NodeType {
        public final ImmutableList<String> labels;
        public final ImmutableSet<String> keys;

        public NodeType(ImmutableList<String> labels, ImmutableSet<String> keys) {
            this.labels = labels;
            this.keys = keys;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            NodeType nodeType = (NodeType) o;
            return Objects.equals(labels, nodeType.labels) &&
                    Objects.equals(keys, nodeType.keys);
        }

        @Override
        public int hashCode() {
            return Objects.hash(labels, keys);
        }

    }

    private final class CreatedNode {
        public final com.livingobjects.neo4j.model.schema.Node node;
        public final org.neo4j.graphdb.Node createdNode;

        public CreatedNode(com.livingobjects.neo4j.model.schema.Node node, org.neo4j.graphdb.Node createdNode) {
            this.node = node;
            this.createdNode = createdNode;
        }
    }

}
