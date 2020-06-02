package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.time.Instant;
import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.OVERRIDE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;

public final class OverridableElementFactory {

    private final GraphDatabaseService graphdb;
    private final Label keyLabel;
    private final ImmutableSet<Label> extraLabel;

    private OverridableElementFactory(GraphDatabaseService graphdb, Label keyLabel, Label... extraLabels) {
        this.graphdb = graphdb;
        this.keyLabel = keyLabel;
        this.extraLabel = ImmutableSet.copyOf(extraLabels);
    }

    public UniqueEntity<Node> getOrOverride(Scope scope, String keyProperty, Object keyValue) {
        try (Transaction tx = graphdb.beginTx()) {
            ImmutableList<String> tmpScopes = ImmutableList.of(scope.tag, SP_SCOPE.tag, GLOBAL_SCOPE.tag);
            ImmutableList<String> scopes = tmpScopes.subList(tmpScopes.lastIndexOf(scope.tag), tmpScopes.size());
            ImmutableMap.Builder<String, Node> expandsBldr = ImmutableMap.builder();
            tx.findNodes(keyLabel, keyProperty, keyValue).forEachRemaining(node -> {
                String nodeScope = getElementScopeFromPlanet(keyProperty, keyValue, node);
                expandsBldr.put(nodeScope, node);
            });
            ImmutableMap<String, Node> expands = expandsBldr.build();

            Node extendedNode = null;
            for (String s : scopes) {
                if (s.equals(scope.tag)) continue;
                Node node = expands.get(s);
                if (node != null) {
                    extendedNode = node;
                    break;
                }
            }

            UniqueEntity<Node> node = Optional.ofNullable(expands.get(scope.tag))
                    .map(UniqueEntity::existing)
                    .orElseGet(() -> UniqueEntity.created(initialize(tx.createNode(), keyProperty, keyValue, scope)));

            // Remove extra label from previous Scopes
            expands.forEach((sc, n) -> {
                if (!scopes.contains(sc))
                    extraLabel.forEach(n::removeLabel);

            });

            if (extendedNode != null) {
                node.entity.setProperty(OVERRIDE, Boolean.TRUE);
                return ensureExtendRelation(node, extendedNode);
            } else {
                extraLabel.forEach(node.entity::addLabel);
                return node;
            }
        }
    }

    private String getElementScopeFromPlanet(String keyProperty, Object keyValue, Node node) {
        Relationship planetRelationship = node.getSingleRelationship(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING);
        if (planetRelationship == null) {
            throw new IllegalArgumentException(String.format("%s %s=%s is not linked to a planet", keyLabel, keyProperty, keyValue));
        }
        return planetRelationship.getEndNode().getProperty(SCOPE).toString();
    }

    private UniqueEntity<Node> ensureExtendRelation(UniqueEntity<Node> node, Node extendedNode) {
        Relationship rel = node.entity.getSingleRelationship(RelationshipTypes.EXTEND, Direction.OUTGOING);
        if (rel != null) {
            Node endNode = rel.getEndNode();
            if (endNode.getId() != extendedNode.getId()) {
                rel.delete();
                rel = null;
            }
        }

        if (rel == null && extendedNode != null) // Add extend link only for override element not for custom
            node.entity.createRelationshipTo(extendedNode, RelationshipTypes.EXTEND);

        return node;
    }

    private Node initialize(Node created, String keyProperty, Object keyValue, Scope scope) {
        created.addLabel(keyLabel);
        created.setProperty(keyProperty, keyValue);
        created.setProperty(SCOPE, scope.tag);
        created.setProperty(GraphModelConstants.CREATED_AT, Instant.now().toEpochMilli());
        return created;
    }

    public static OverridableElementFactory networkElementFactory(GraphDatabaseService graphdb) {
        return new OverridableElementFactory(graphdb, Labels.ELEMENT, Labels.NETWORK_ELEMENT);
    }


}
