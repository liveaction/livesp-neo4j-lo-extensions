package com.livingobjects.neo4j.iwan.model.schema;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.iwan.model.UniqueEntity;
import com.livingobjects.neo4j.iwan.model.exception.SchemaTemplateException;
import com.livingobjects.neo4j.iwan.model.schema.factories.CustomNodeFactory;
import com.livingobjects.neo4j.iwan.model.schema.model.Node;
import com.livingobjects.neo4j.iwan.model.schema.model.Property;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaTemplate;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaVersion;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Objects;

public final class SchemaTemplateLoader {

    public static final DynamicRelationshipType APPLIED_TO_LINK = DynamicRelationshipType.withName("AppliedTo");
    private final GraphDatabaseService graphDB;

    private final Map<NodeType, CustomNodeFactory> nodeFactories = Maps.newHashMap();

    public SchemaTemplateLoader(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
    }

    public void loadAndApplyTemplate(InputStream csv, InputStream xmlTemplate) throws IOException {
        CSVReader reader = new CSVReader(new InputStreamReader(csv));
        ImmutableMap<String, Integer> header = readCsvHeader(reader);

        SchemaTemplate template = parseTemplate(xmlTemplate);

        String[] line = reader.readNext();
        while (line != null) {

            applyTemplate(template, header, line);

            line = reader.readNext();
        }

    }

    private void applyTemplate(SchemaTemplate template, ImmutableMap<String, Integer> header, String[] line) {
        UniqueEntity<org.neo4j.graphdb.Node> templateNode = createNode(template.templateNode, header, line);

        if (shouldApplyTemplate(template, templateNode)) {
            Node templateVersionNode = new Node(ImmutableList.of("Template"), ImmutableMap.of("name", template.name, "version", template.version.toString()), ImmutableSet.of());
            CustomNodeFactory factory = nodeFactory(templateVersionNode);
            UniqueEntity<org.neo4j.graphdb.Node> node = factory.getOrCreate(ImmutableSet.of(template.name, template.version.toString()));
            node.entity.createRelationshipTo(templateNode.entity, APPLIED_TO_LINK);


        }
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

    private UniqueEntity<org.neo4j.graphdb.Node> createNode(Node node, ImmutableMap<String, Integer> header, String[] line) {
        CustomNodeFactory factory = nodeFactory(node);

        ImmutableSet.Builder<String> transformedKeys = ImmutableSet.builder();
        for (Map.Entry<String, String> keyProperty : node.keys.entrySet()) {
            String value = keyProperty.getValue();
            String transformedValue = StringTemplate.template(value, header, line);
            transformedKeys.add(transformedValue);
        }

        UniqueEntity<org.neo4j.graphdb.Node> entity = factory.getOrCreate(transformedKeys.build());
        if (entity.wasCreated) {
            for (Property property : node.properties) {
                String value = StringTemplate.template(property.value, header, line);
                entity.entity.setProperty(property.name, value);
            }
        }
        return entity;
    }

    private SchemaTemplate parseTemplate(InputStream xmlTemplate) throws SchemaTemplateException {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        try {
            SAXParser saxParser = factory.newSAXParser();
            XMLSchemaTemplateHandler handler = new XMLSchemaTemplateHandler();
            saxParser.parse(xmlTemplate, handler);
            return handler.getTemplate();
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
                headerBuilder.put(column, index);
            }
        }
        return headerBuilder.build();
    }

    private CustomNodeFactory nodeFactory(Node node) {
        ImmutableSet<String> keys = node.keys.keySet();
        NodeType nodeType = new NodeType(node.labels, keys);
        return nodeFactories.computeIfAbsent(nodeType, k -> new CustomNodeFactory(graphDB, node.labels, keys));
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

}
