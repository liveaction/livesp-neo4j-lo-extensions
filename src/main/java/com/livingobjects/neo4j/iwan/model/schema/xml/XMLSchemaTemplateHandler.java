package com.livingobjects.neo4j.iwan.model.schema.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.iwan.model.schema.model.Node;
import com.livingobjects.neo4j.iwan.model.schema.model.Property;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaTemplate;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaVersion;
import com.livingobjects.neo4j.iwan.model.schema.model.relationships.Relationship;
import com.livingobjects.neo4j.iwan.model.schema.model.relationships.Relationships;
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
                Property.Type type = Optional.ofNullable(attributes.getValue("type"))
                        .map(Property.Type::valueOf)
                        .orElse(Property.Type.STRING);
                Property property = new Property(name, value, type);
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
            case "relationship":
                currentRelationshipsBuilder.relationships.add(newRelationship(attributes));
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

    private Relationship newRelationship(Attributes attributes) throws SAXException {
        String nodeRef = attributes.getValue("node");
        String cypher = attributes.getValue("cypher");
        if (nodeRef == null && cypher == null) {
            throw new SAXException("The relationship must have an attribute 'node' or 'cypher'. Parent node : " + displayCurrentNode());
        }
        if (nodeRef != null && cypher != null) {
            throw new SAXException("The relationship cannot have both 'node' and 'cypher' attribute. Parent node : " + displayCurrentNode());
        }
        ImmutableSet.Builder<Property> propertiesBuilder = ImmutableSet.builder();
        for (int index = 0; index < attributes.getLength(); index++) {
            String attributeName = attributes.getQName(index);
            if (!attributeName.equals("node") && !attributeName.equals("cypher")) {
                String value = attributes.getValue(index);
                propertiesBuilder.add(new Property(attributeName, value, Property.Type.STRING));
            }
        }
        if (nodeRef != null) {
            return Relationship.node(propertiesBuilder.build(), nodeRef);
        } else {
            return Relationship.cypher(propertiesBuilder.build(), cypher);
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
