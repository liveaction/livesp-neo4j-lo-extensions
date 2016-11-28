package com.livingobjects.neo4j.iwan.model.schema.factories;

import com.livingobjects.neo4j.iwan.model.CacheIndexedElementFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.time.Instant;
import java.util.Iterator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.CREATED_AT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_ATTRIBUTE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants._TYPE;

public final class AttributeFactory extends CacheIndexedElementFactory<String> {

    public AttributeFactory(GraphDatabaseService graphDB) {
        super(graphDB);
    }

    @Override
    protected Iterator<Node> initialLoad() {
        return graphDB.findNodes(LABEL_ATTRIBUTE);
    }

    @Override
    protected String readNode(Node node) {
        String type = getNullableStringProperty(node, _TYPE);
        String name = getNullableStringProperty(node, NAME);
        if (type != null && name != null) {
            return type + ':' + name;
        } else {
            return null;
        }
    }

    @Override
    protected Node createNode(String attribute) {
        String[] split = attribute.split(":");
        if (split.length == 2) {
            String type = split[0];
            String name = split[1];
            Node node = graphDB.createNode();
            node.addLabel(LABEL_ATTRIBUTE);
            node.setProperty(_TYPE, type);
            node.setProperty(NAME, name);
            node.setProperty(CREATED_AT, Instant.now().toEpochMilli());
            return node;
        } else {
            throw new IllegalArgumentException("Attribute key must be of the form 'type:name' : " + attribute);
        }
    }
}

