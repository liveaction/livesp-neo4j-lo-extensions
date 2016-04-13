package com.livingobjects.neo4j.iwan.model;

import com.google.common.collect.ImmutableMap;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.time.Instant;
import java.util.Map;

import static com.livingobjects.cosmos.shared.model.GraphNodeLabel.ELEMENT;
import static com.livingobjects.cosmos.shared.model.GraphNodeLabel.NETWORK_ELEMENT;
import static com.livingobjects.cosmos.shared.model.GraphNodeProperties.CREATED_AT;
import static com.livingobjects.cosmos.shared.model.GraphNodeProperties.TAG;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_NETWORK_ELEMENT;

public final class NetworkElementFactory {
    private final GraphDatabaseService graphdb;
    private final Label label;

    private NetworkElementFactory(GraphDatabaseService graphdb, Label label) {
        this.graphdb = graphdb;
        this.label = label;
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(String property, Object value) {
        Node node = graphdb.findNode(label, property, value);
        if (node != null) {
            return new UniqueEntity<>(false, node);
        }

        node = graphdb.createNode();
        return new UniqueEntity<>(true, initialize(node, ImmutableMap.of(property, value)));
    }

    private Node initialize(Node created, Map<String, Object> properties) {
        created.addLabel(DynamicLabel.label(NETWORK_ELEMENT));
        created.addLabel(DynamicLabel.label(ELEMENT));
        created.setProperty(TAG, properties.get(TAG));
        created.setProperty(CREATED_AT, Instant.now().toEpochMilli());

        return created;
    }

    public static NetworkElementFactory build(GraphDatabaseService graphdb) {
        return new NetworkElementFactory(graphdb, LABEL_NETWORK_ELEMENT);
    }

    public static final class UniqueEntity<T> {
        public final boolean wasCreated;
        public final T entity;

        UniqueEntity(boolean wasCreated, T entity) {
            this.wasCreated = wasCreated;
            this.entity = entity;
        }
    }
}
