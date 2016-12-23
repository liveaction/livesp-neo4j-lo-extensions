package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.iwan.model.schema.model.Graph;
import com.livingobjects.neo4j.iwan.model.schema.model.Node;
import com.livingobjects.neo4j.iwan.model.schema.model.Property;
import com.livingobjects.neo4j.iwan.model.schema.model.Relationships;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaTemplate;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaVersion;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public final class XMLSchemaTemplateHandler extends DefaultHandler {

    private String name;

    private SchemaVersion version;

    private final Map<String, PendingNode> nodesByIds = Maps.newHashMap();

    private PendingNode templateNode;

    private PendingNode currentNode;

    private PendingRelationships currentRelationships;

    public final Set<PendingNode> nodes = Sets.newHashSet();

    public final Set<PendingRelationships> relationships = Sets.newHashSet();

    public class PendingNode {

        public final Optional<String> id;

        public final ImmutableList<String> labels;

        public final ImmutableMap<String, String> keys;

        public final Set<Property> properties = Sets.newHashSet();

        public final Set<PendingRelationships> relationships = Sets.newHashSet();

        public PendingNode(Optional<String> id, ImmutableList<String> labels, ImmutableMap<String, String> keys) {
            this.id = id;
            this.labels = labels;
            this.keys = keys;
        }
    }

    public class PendingRelationships {

        public final String type;

        public final Relationships.Direction to;

        public final Set<PendingRelationship> relationships = Sets.newHashSet();

        public PendingRelationships(String type, Relationships.Direction to) {
            this.type = type;
            this.to = to;
        }
    }

    public class PendingRelationship {

        public final String nodeRef;

        public final ImmutableSet<Property> properties;

        public PendingRelationship(String nodeRef, ImmutableSet<Property> properties) {
            this.nodeRef = nodeRef;
            this.properties = properties;
        }
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        switch (qName) {
            case "template":
                name = attributes.getValue("name");
                version = SchemaVersion.of(attributes.getValue("version"));
                break;
            case "templateNode":
                templateNode = newNode(attributes);
                break;
            case "node":
                currentNode = newNode(attributes);
                break;
            case "property":
                String name = attributes.getValue("name");
                String value = attributes.getValue("value");
                Property.Type type = Optional.ofNullable(attributes.getValue("type"))
                        .map(Property.Type::valueOf)
                        .orElse(Property.Type.STRING);
                Property property = new Property(name, value, type);
                if (currentNode == null) {
                    if (templateNode != null) {
                        templateNode.properties.add(property);
                    }
                } else {
                    currentNode.properties.add(property);
                }
                break;
            case "relationships":
                currentRelationships = newPendingRelationships(attributes);
                break;
            case "relationship":
                currentRelationships.relationships.add(newPendingRelationship(attributes));
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
                templateNode.id.ifPresent(id -> nodesByIds.put(id, templateNode));
                nodes.add(templateNode);
                templateNode = null;
                break;
            case "relationships":
                relationships.add(currentRelationships);
                currentRelationships = null;
                break;
            case "node":
                currentNode.id.ifPresent(id -> nodesByIds.put(id, currentNode));
                nodes.add(currentNode);
                currentNode = null;
        }
    }

    private PendingRelationships newPendingRelationships(Attributes attributes) {
        String type = attributes.getValue("type");
        Relationships.Direction direction = Relationships.Direction.valueOf(attributes.getValue("direction"));
        return new PendingRelationships(type, direction);
    }

    private PendingRelationship newPendingRelationship(Attributes attributes) {
        String nodeRef = attributes.getValue("node");
        ImmutableSet.Builder<Property> propertiesBuilder = ImmutableSet.builder();
        for (int index = 0; index < attributes.getLength(); index++) {
            String attributeName = attributes.getQName(index);
            if (!attributeName.equals("type") && !attributeName.equals("from") && !attributeName.equals("to")) {
                String value = attributes.getValue(index);
                propertiesBuilder.add(new Property(attributeName, value, Property.Type.STRING));
            }
        }
        return new PendingRelationship(nodeRef, propertiesBuilder.build());
    }

    private PendingNode newNode(Attributes attributes) {
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
        return new PendingNode(id, labels, keysBuilder.build());
    }

    public SchemaTemplate getTemplate() throws SAXException {
        if (templateNode == null) {
            throw new SAXException("templateNode element is required");
        }
        ImmutableSet.Builder<Relationship> relationships = ImmutableSet.builder();
        for (PendingRelationship pendingRelationship : pendingGraph.relationships) {
            Node from = nodesByIds.get(pendingRelationship.from);
            Node to = nodesByIds.get(pendingRelationship.to);
            if (from != null && to != null) {
                relationships.add(new Relationship(from, to, pendingRelationship.type, pendingRelationship.properties));
            }
        }
        ImmutableSet.Builder<Node> templateNodes = ImmutableSet.builder();
        Node node = buildNode(templateNode);
        return new SchemaTemplate(name, version, templateNode, templateNodes);
    }

    private Node buildNode(PendingNode templateNode) {

        return new Node(templateNode.labels, templateNode.keys, ImmutableSet.copyOf(templateNode.properties), relationships);
    }

}
