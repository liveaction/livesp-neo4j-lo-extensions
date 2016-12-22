package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.iwan.model.schema.model.Graph;
import com.livingobjects.neo4j.iwan.model.schema.model.Node;
import com.livingobjects.neo4j.iwan.model.schema.model.Property;
import com.livingobjects.neo4j.iwan.model.schema.model.Relationship;
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

    private final Map<String, Node> nodesByIds = Maps.newHashMap();

    private Node templateNode;

    private Map<String, Graph> sections = Maps.newHashMap();


    public class RelationshipBuilder {

        public final String from;

        public final String to;

        public final ImmutableList<String> labels;

        public final ImmutableMap.Builder<String, String> keys = ImmutableMap.builder();

        private final ImmutableSet.Builder<Property> properties = ImmutableSet.builder();

        Relationship build() {
            return new Relationship(type, properties.build());
        }
    }

    public class NodeBuilder {

        private final ImmutableList.Builder<String> labels = ImmutableList.builder();

        private final ImmutableMap.Builder<String, String> keys = ImmutableMap.builder();

        private final ImmutableSet.Builder<Property> properties = ImmutableSet.builder();

        Node build() {
            return new Node(labels.build(), keys.build(), properties.build());
        }
    }

    public class GraphBuilder {

        public final String name;

        public final Set<Node> nodes = Sets.newHashSet();

        public final Set<Relationship> relationships = Sets.newHashSet();

        public GraphBuilder(String name) {
            this.name = name;
        }

        public void addNode(Optional<String> _id, Node node) {
            nodes.add(_id, node);
        }

        public void addRelationship(Relationship relationship) {
            relationships.add(relationship);
        }

        Graph build() {
            return new Graph(ImmutableSet.copyOf(nodes), ImmutableSet.copyOf(relationships));
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
                break;
            case "section":
                currentSection = new Graph();
        }
    }

    public SchemaTemplate getTemplate() {
        return new SchemaTemplate(name, version, null, null);
    }
}
