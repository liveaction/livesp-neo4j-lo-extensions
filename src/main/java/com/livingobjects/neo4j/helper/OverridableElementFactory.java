package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

public final class OverridableElementFactory {
    private final GraphDatabaseService graphdb;
    private final Label keyLabel;
    private final ImmutableSet<Label> extraLabel;

    public OverridableElementFactory(
            GraphDatabaseService graphdb, Label keyLabel, Label... extraLabels) {
        this.graphdb = graphdb;
        this.keyLabel = keyLabel;
        this.extraLabel = ImmutableSet.copyOf(extraLabels);
    }

    public UniqueEntity<Node> getOrOverride(Scope scope, String keyProperty, Object keyValue) {
        ImmutableList<String> scopes = ImmutableList.of(scope.tag, SP_SCOPE.tag, GLOBAL_SCOPE.tag);
        Map<String, Node> expands = Maps.newHashMapWithExpectedSize(scopes.size());
        graphdb.findNodes(keyLabel, keyProperty, keyValue).forEachRemaining(node -> {
            String nodeScope = (String) node.getProperty(_SCOPE, GLOBAL_SCOPE.tag);
            if (scopes.contains(nodeScope)) {
                expands.put(nodeScope, node);
            }
        });

        Node extendedNode = Optional.ofNullable(expands.get(SP_SCOPE.tag))
                .orElseGet(() -> expands.get(GLOBAL_SCOPE.tag));
        boolean override = (extendedNode != null);

        if (expands.isEmpty())
            return UniqueEntity.created(initialize(graphdb.createNode(), keyProperty, keyValue, scope, override));

        UniqueEntity<Node> node = Optional.ofNullable(expands.get(scope.tag))
                .map(UniqueEntity::existing)
                .orElseGet(() -> UniqueEntity.created(initialize(graphdb.createNode(), keyProperty, keyValue, scope, override)));

        return (override)
                ? ensureExtendRelation(node, extendedNode)
                : node;
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

    private Node initialize(Node created, String keyProperty, Object keyValue, Scope scope, boolean override) {
        created.addLabel(keyLabel);
        if (!override)
            extraLabel.forEach(created::addLabel);

        created.setProperty(keyProperty, keyValue);
        created.setProperty(_SCOPE, scope.tag);
        created.setProperty(IwanModelConstants.CREATED_AT, Instant.now().toEpochMilli());
        return created;
    }

    public static OverridableElementFactory networkElementFactory(GraphDatabaseService graphdb) {
        return new OverridableElementFactory(graphdb, Labels.ELEMENT, Labels.NETWORK_ELEMENT);
    }

}
