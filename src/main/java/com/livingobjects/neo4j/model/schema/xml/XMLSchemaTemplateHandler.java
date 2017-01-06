package com.livingobjects.neo4j.model.schema.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.PropertyType;
import com.livingobjects.neo4j.model.schema.Node;
import com.livingobjects.neo4j.model.schema.Property;
import com.livingobjects.neo4j.model.schema.SchemaTemplate;
import com.livingobjects.neo4j.model.schema.SchemaVersion;
import com.livingobjects.neo4j.model.schema.relationships.Relationship;
import com.livingobjects.neo4j.model.schema.relationships.Relationships;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Optional;
import java.util.Set;

public final class XMLSchemaTemplateHandler extends DefaultHandler {

    private String name;
    private SchemaVersion version;
    private Node templateNode;
    private final Set<Node> nodes = Sets.newHashSet();

    private NodeBuilder templateNodeBuilder;
    private NodeBuilder currentNodeBuilder;
    private RelationshipsBuilder currentRelationshipsBuilder;

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        switch (qName) {
            case "template":
                name = attributes.getValue("name");
                version = SchemaVersion.of(attributes.getValue("version"));
                break;
            case "templateNode":
                templateNodeBuilder = newNode(attributes);
                break;
            case "node":
                currentNodeBuilder = newNode(attributes);
                break;
            case "property":
                String name = attributes.getValue("name");
                String value = attributes.getValue("value");
                PropertyType type = Optional.ofNullable(attributes.getValue("type"))
                        .map(PropertyType::valueOf)
                        .orElse(PropertyType.STRING);
                String isArrayAttribute = attributes.getValue("isArray");
                boolean isArray = false;
                if (isArrayAttribute != null) {
                    isArray = Boolean.valueOf(isArrayAttribute);
                }
                Property property = new Property(name, value, type, isArray);
                if (currentNodeBuilder == null) {
                    if (templateNodeBuilder != null) {
                        templateNodeBuilder.properties.add(property);
                    }
                } else {
                    currentNodeBuilder.properties.add(property);
                }
                break;
            case "relationships":
                currentRelationshipsBuilder = newPendingRelationships(attributes);
                break;
            case "relationshipHook":
                currentRelationshipsBuilder.relationships.add(newRelationship(attributes, true));
                break;
            case "relationship":
                currentRelationshipsBuilder.relationships.add(newRelationship(attributes, false));
                break;
            case "section":
                break;
            default:
                throw new SAXException("Unknown element : " + qName);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (qName) {
            case "templateNode":
                templateNode = templateNodeBuilder.build();
                nodes.add(templateNode);
                templateNodeBuilder = null;
                break;
            case "relationships":
                if (currentNodeBuilder == null) {
                    if (templateNodeBuilder != null) {
                        templateNodeBuilder.relationships.add(currentRelationshipsBuilder.build());
                    }
                } else {
                    currentNodeBuilder.relationships.add(currentRelationshipsBuilder.build());
                }
                currentRelationshipsBuilder = null;
                break;
            case "node":
                Node node = currentNodeBuilder.build();
                nodes.add(node);
                currentNodeBuilder = null;
                break;
        }
    }

    private RelationshipsBuilder newPendingRelationships(Attributes attributes) {
        String type = attributes.getValue("type");
        Relationships.Direction direction = Relationships.Direction.valueOf(attributes.getValue("direction"));
        String replaceAttribute = attributes.getValue("replace");
        boolean replace = true;
        if (replaceAttribute != null) {
            replace = Boolean.valueOf(replaceAttribute);
        }
        return new RelationshipsBuilder(type, direction, replace);
    }

    private Relationship newRelationship(Attributes attributes, boolean hook) throws SAXException {

        ImmutableSet.Builder<Property> propertiesBuilder = ImmutableSet.builder();
        for (int index = 0; index < attributes.getLength(); index++) {
            String attributeName = attributes.getQName(index);
            if (!attributeName.equals("node") && !attributeName.equals("cypher")) {
                String value = attributes.getValue(index);
                propertiesBuilder.add(new Property(attributeName, value, PropertyType.STRING, false));
            }
        }

        if (hook) {
            String cypher = attributes.getValue("cypher");
            if (cypher == null) {
                throw new SAXException("The relationshipHook must have a 'cypher' attribute. Parent node : " + displayCurrentNode());
            }
            return Relationship.cypher(propertiesBuilder.build(), cypher);
        } else {
            String nodeRef = attributes.getValue("node");
            if (nodeRef == null) {
                throw new SAXException("The relationship must have a 'node' attribute. Parent node : " + displayCurrentNode());
            }
            return Relationship.node(propertiesBuilder.build(), nodeRef);
        }
    }

    private String displayCurrentNode() {
        return templateNodeBuilder != null ?
                templateNodeBuilder.id.orElse(MoreObjects.toStringHelper(templateNodeBuilder)
                        .add("labels", templateNodeBuilder.labels)
                        .add("keys", templateNodeBuilder.keys)
                        .toString()) :
                currentNodeBuilder.id.orElse(MoreObjects.toStringHelper(currentNodeBuilder)
                        .add("labels", currentNodeBuilder.labels)
                        .add("keys", currentNodeBuilder.keys)
                        .toString());
    }

    private NodeBuilder newNode(Attributes attributes) {
        Optional<String> id = Optional.ofNullable(attributes.getValue("_id"));
        String labelsAttr = attributes.getValue("labels");
        ImmutableList<String> labels = ImmutableList.copyOf(Splitter.on(',').omitEmptyStrings().splitToList(labelsAttr));
        ImmutableMap.Builder<String, String> keysBuilder = ImmutableMap.builder();
        for (int index = 0; index < attributes.getLength(); index++) {
            String attributeName = attributes.getQName(index);
            if (!attributeName.equals("_id") && !attributeName.equals("labels")) {
                String value = attributes.getValue(index);
                keysBuilder.put(attributeName, value);
            }
        }
        return new NodeBuilder(id, labels, keysBuilder.build());
    }

    public SchemaTemplate getTemplate() throws SAXException {
        if (templateNode == null) {
            throw new SAXException("templateNode element is required");
        }
        return new SchemaTemplate(name, version, templateNode, ImmutableSet.copyOf(nodes));
    }

    private class NodeBuilder {

        public final Optional<String> id;

        public final ImmutableList<String> labels;

        public final ImmutableMap<String, String> keys;

        public final Set<Property> properties = Sets.newHashSet();

        public final Set<Relationships> relationships = Sets.newHashSet();

        public NodeBuilder(Optional<String> id, ImmutableList<String> labels, ImmutableMap<String, String> keys) {
            this.id = id;
            this.labels = labels;
            this.keys = keys;
        }

        public Node build() {
            return new Node(id, labels, keys, ImmutableSet.copyOf(properties), ImmutableSet.copyOf(relationships));
        }
    }

    private class RelationshipsBuilder {

        public final String type;

        public final Relationships.Direction direction;

        public final boolean replace;

        public final Set<Relationship> relationships = Sets.newHashSet();

        public RelationshipsBuilder(String type, Relationships.Direction direction, boolean replace) {
            this.type = type;
            this.direction = direction;
            this.replace = replace;
        }

        public Relationships build() {
            return new Relationships(type, direction, replace, ImmutableSet.copyOf(relationships));
        }
    }

}
