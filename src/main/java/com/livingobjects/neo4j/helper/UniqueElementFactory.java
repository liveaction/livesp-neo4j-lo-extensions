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

    public Node getWithOutcome(String keyProperty, Object keyValue, Transaction tx) {
        UniqueEntity<Node> entity = getOrCreateWithOutcome(false, keyProperty, keyValue, null, null, null, tx);
        return entity == null ? null : entity.entity;
    }

    /**
     * Only this one method has the username parameter, because it's used by the CsvTopologyLoader to create network elements,
     * which is the only use case where we want the username related information (such as createdBy or updatedBy).
     * For relations between network elements, for the metaschema or for creating planets/realms for example,
     * we do not care about the username related information (it would always be system, and would be redundant)
     */
    public UniqueEntity<Node> getOrCreateWithOutcome(String keyProperty, Object keyValue, String username, Transaction tx) {
        return getOrCreateWithOutcome(true, keyProperty, keyValue, null, null, username, tx);
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(String keyProperty, Object keyValue, Transaction tx) {
        return getOrCreateWithOutcome(true, keyProperty, keyValue, null, null, null, tx);
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(String key1, Object value1, String key2, Object value2, Transaction tx) {
        return getOrCreateWithOutcome(true, key1, value1, key2, value2, null, tx);
    }

    public UniqueEntity<Node> getOrCreateWithOutcome(MatchProperties matchProperties, Transaction tx) {
        return getOrCreateWithOutcome(true, matchProperties.key1, matchProperties.value1, matchProperties.key2, matchProperties.value2, null, tx);
    }

    public synchronized UniqueEntity<Relationship> getOrCreateRelation(boolean createIfNotExists, Node from, Node to, RelationshipType type) {
        UniqueEntity<Relationship> relation = null;
        for (Relationship r : from.getRelationships(Direction.OUTGOING, type)) {
            if (r.getEndNode().equals(to)) { // put other conditions here, if needed
                relation = UniqueEntity.existing(r);
                break;
            }
        }
        if (relation == null && createIfNotExists) {
            relation = UniqueEntity.created(from.createRelationshipTo(to, type));
        }
        return relation;
    }

    private synchronized UniqueEntity<Node> getOrCreateWithOutcome(boolean createIfNotExist, String key1, Object value1, String key2, Object value2,
                                                                   String username, Transaction tx) {
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
            return UniqueEntity.created(initialize(node, key1, value1, key2, value2, username));
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

    private Node initialize(Node created, String key1, Object value1, String key2, Object value2, String username) {
        created.addLabel(keyLabel);
        extraLabel.ifPresent(created::addLabel);
        created.setProperty(key1, value1);
        if (key2 != null) {
            created.setProperty(key2, value2);
        }
        created.setProperty(GraphModelConstants.CREATED_AT, Instant.now().toEpochMilli());
        created.setProperty(GraphModelConstants.UPDATED_AT, Instant.now().toEpochMilli());

        Optional.ofNullable(username)
                .filter(usernameValue -> !usernameValue.isBlank())
                .ifPresent(usernameValue -> {
                    created.setProperty(GraphModelConstants.CREATED_BY, usernameValue);
                    created.setProperty(GraphModelConstants.UPDATED_BY, usernameValue);
                });
        return created;
    }

    public static UniqueElementFactory networkElementFactory(GraphDatabaseService graphdb) {
        return new UniqueElementFactory(graphdb, Labels.NETWORK_ELEMENT, Optional.of(Labels.ELEMENT));
    }
}
