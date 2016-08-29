package com.livingobjects.neo4j.iwan.model;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.UnmodifiableIterator;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.CREATED_AT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_ATTRIBUTE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_COUNTER;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_ELEMENT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_NETWORK_ELEMENT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_PLANET;

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
        return getOrCreateWithOutcome(ImmutableMap.of(keyProperty, keyValue));
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(ImmutableMap<String, Object> keyProperties) {
        if (keyProperties.isEmpty()) {
            throw new IllegalArgumentException("At least one key must be given to retrieve/create a node");
        }
        UnmodifiableIterator<Map.Entry<String, Object>> iterator = keyProperties.entrySet().iterator();
        Map.Entry<String, Object> firstKey = iterator.next();
        Node node;
        if (iterator.hasNext()) {
            node = filterNode(keyLabel, firstKey.getKey(), firstKey.getValue(), iterator);
        } else {
            node = graphdb.findNode(keyLabel, firstKey.getKey(), firstKey.getValue());
        }
        if (node != null) {
            return new UniqueEntity<>(false, node);
        }

        node = graphdb.createNode();
        return new UniqueEntity<>(true, initialize(node, keyProperties));
    }

    private Node filterNode(Label keyLabel, String firstKey, Object firstKeyValue, UnmodifiableIterator<Map.Entry<String, Object>> iterator) {
        ResourceIterator<Node> nodes = graphdb.findNodes(keyLabel, firstKey, firstKeyValue);
        while (nodes.hasNext()) {
            Node next = nodes.next();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> property = iterator.next();
                Object currentValue = next.getProperty(property.getKey(), null);
                if (property.getValue().equals(currentValue)) {
                    return next;
                }
            }
        }
        return null;
    }

    private Node initialize(Node created, ImmutableMap<String, Object> keyProperties) {
        created.addLabel(keyLabel);
        extraLabel.ifPresent(created::addLabel);
        for (Map.Entry<String, Object> keyProperty : keyProperties.entrySet()) {
            created.setProperty(keyProperty.getKey(), keyProperty.getValue());
        }
        created.setProperty(CREATED_AT, Instant.now().toEpochMilli());
        return created;
    }

    public static UniqueElementFactory networkElementFactory(GraphDatabaseService graphdb) {
        return new UniqueElementFactory(graphdb, LABEL_NETWORK_ELEMENT, Optional.of(LABEL_ELEMENT));
    }

    public static UniqueElementFactory attributeFactory(GraphDatabaseService graphdb) {
        return new UniqueElementFactory(graphdb, LABEL_ATTRIBUTE, Optional.empty());
    }

    public static UniqueElementFactory planetFactory(GraphDatabaseService graphDb) {
        return new UniqueElementFactory(graphDb, LABEL_PLANET, Optional.empty());
    }

    public static UniqueElementFactory counterFactory(GraphDatabaseService graphDb) {
        return new UniqueElementFactory(graphDb, LABEL_COUNTER, Optional.empty());
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
