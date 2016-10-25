package com.livingobjects.neo4j.iwan.model.schema.factories;

import com.livingobjects.neo4j.iwan.model.CacheIndexedElementFactory;
import com.livingobjects.neo4j.iwan.model.schema.Attribute;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.time.Instant;
import java.util.Iterator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.CREATED_AT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_ATTRIBUTE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants._TYPE;

public final class AttributeFactory extends CacheIndexedElementFactory<Attribute> {

    private static final String SPECIALIZER = "specializer";

    public AttributeFactory(GraphDatabaseService graphDB) {
        super(graphDB);
    }

    @Override
    protected Iterator<Node> initialLoad() {
        return graphDB.findNodes(LABEL_ATTRIBUTE);
    }

    @Override
    protected Attribute readNode(Node node) {
        String type = getNullableStringProperty(node, _TYPE);
        String name = getNullableStringProperty(node, NAME);
        String specializer = getNullableStringProperty(node, SPECIALIZER);
        if (type != null && name != null) {
            return new Attribute(type, name, specializer);
        } else {
            return null;
        }
    }

    @Override
    protected Node createNode(Attribute attribute) {
        Node node = graphDB.createNode();
        node.addLabel(LABEL_ATTRIBUTE);
        node.setProperty(_TYPE, attribute.type);
        node.setProperty(NAME, attribute.name);
        if (attribute.specializer != null) {
            node.setProperty(SPECIALIZER, attribute.specializer);
        }
        node.setProperty(CREATED_AT, Instant.now().toEpochMilli());
        return node;
    }
}

