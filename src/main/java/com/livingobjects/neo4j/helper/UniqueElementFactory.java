package com.livingobjects.neo4j.helper;

import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

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

    public Label keyLabel() {
        return keyLabel;
    }

    public Node getWithOutcome(String keyProperty, Object keyValue, Transaction tx) {
        return getWithOutcome(keyProperty, keyValue, null, null, tx);
    }

    public Node getWithOutcome(MatchProperties matchProperties, Transaction tx) {
        return getWithOutcome(matchProperties.key1, matchProperties.value1, matchProperties.key2, matchProperties.value2, tx);
    }

    public Node getWithOutcome(String key1, Object value1, String key2, Object value2, Transaction tx) {
        UniqueEntity<Node> entity = getOrCreateWithOutcome(false, key1, value1, key2, value2, tx);
        return entity == null ? null : entity.entity;
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(String keyProperty, Object keyValue, Transaction tx) {
        return getOrCreateWithOutcome(true, keyProperty, keyValue, null, null, tx);
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(String key1, Object value1, String key2, Object value2, Transaction tx) {
        return getOrCreateWithOutcome(true, key1, value1, key2, value2, tx);
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(MatchProperties matchProperties, Transaction tx) {
        return getOrCreateWithOutcome(true, matchProperties.key1, matchProperties.value1, matchProperties.key2, matchProperties.value2, tx);
    }

    public synchronized UniqueEntity<Relationship> getOrCreateRelation(Node from, Node to, RelationshipType type) {
        UniqueEntity<Relationship> relation = null;
        for (Relationship r : from.getRelationships(Direction.OUTGOING, type)) {
            if (r.getEndNode().equals(to)) { // put other conditions here, if needed
                relation = UniqueEntity.existing(r);
                break;
            }
        }
        if (relation == null) {
            relation = UniqueEntity.created(from.createRelationshipTo(to, type));
        }
        return relation;
    }

    private synchronized UniqueEntity<Node> getOrCreateWithOutcome(boolean createIfNotExist, String key1, Object value1, String key2, Object value2, Transaction tx) {
        Node node;
        if (key2 == null) {
            node = tx.findNode(keyLabel, key1, value1);
        } else {
            node = filterNode(key1, value1, key2, value2, tx);
        }
        if (node != null) {
            return UniqueEntity.existing(node);
        }

        if (createIfNotExist) {
            node = tx.createNode();
            return UniqueEntity.created(initialize(node, key1, value1, key2, value2));
        } else {
            return null;
        }
    }

    private Node filterNode(String key1, Object value1, String key2, Object value2, Transaction tx) {
        ResourceIterator<Node> nodes = tx.findNodes(keyLabel, key1, value1);
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
        created.setProperty(GraphModelConstants.CREATED_AT, Instant.now().toEpochMilli());
        return created;
    }

    public static UniqueElementFactory networkElementFactory(GraphDatabaseService graphdb) {
        return new UniqueElementFactory(graphdb, Labels.NETWORK_ELEMENT, Optional.of(Labels.ELEMENT));
    }
}
