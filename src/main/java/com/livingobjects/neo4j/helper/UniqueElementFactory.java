package com.livingobjects.neo4j.helper;

import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import java.time.Instant;
import java.util.Optional;

public final class UniqueElementFactory {
    private final GraphDatabaseService graphdb;
    private final Label keyLabel;
    private final Optional<Label> extraLabel;

    public UniqueElementFactory(GraphDatabaseService graphdb,
                                Label keyLabel,
                                Optional<Label> extraLabel) {
        this.graphdb = graphdb;
        this.keyLabel = keyLabel;
        this.extraLabel = extraLabel;
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(String keyProperty, Object keyValue) {
        return getOrCreateWithOutcome(keyProperty, keyValue, null, null);
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(String key1, Object value1, String key2, Object value2) {
        Node node;
        if (key2 == null) {
            node = graphdb.findNode(keyLabel, key1, value1);
        } else {
            node = filterNode(key1, value1, key2, value2);
        }
        if (node != null) {
            return UniqueEntity.existing(node);
        }

        node = graphdb.createNode();
        return UniqueEntity.created(initialize(node, key1, value1, key2, value2));
    }

    private Node filterNode(String key1, Object value1, String key2, Object value2) {
        ResourceIterator<Node> nodes = graphdb.findNodes(keyLabel, key1, value1);
        while (nodes.hasNext()) {
            Node next = nodes.next();
            Object currentValue = next.getProperty(key2, null);
            if (value2.equals(currentValue)) {
                return next;
            }
        }
        return null;
    }

    private Node initialize(Node created, String key1, Object value1, String key2, Object value2) {
        created.addLabel(keyLabel);
        extraLabel.ifPresent(created::addLabel);
        created.setProperty(key1, value1);
        if (key2 != null) {
            created.setProperty(key2, value2);
        }
        created.setProperty(IwanModelConstants.CREATED_AT, Instant.now().toEpochMilli());
        return created;
    }

    public static UniqueElementFactory networkElementFactory(GraphDatabaseService graphdb) {
        return new UniqueElementFactory(graphdb, IwanModelConstants.LABEL_NETWORK_ELEMENT, Optional.of(IwanModelConstants.LABEL_ELEMENT));
    }

}
