package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import jline.internal.Nullable;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.*;

public final class OverridableElementFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(OverridableElementFactory.class);

    private final GraphDatabaseService graphdb;
    private final Label keyLabel;
    private final ImmutableSet<Label> extraLabel;

    private OverridableElementFactory(GraphDatabaseService graphdb, Label keyLabel, Label... extraLabels) {
        this.graphdb = graphdb;
        this.keyLabel = keyLabel;
        this.extraLabel = ImmutableSet.copyOf(extraLabels);
    }

    public UniqueEntity<Node> getOrOverride(@Nullable Scope nullableScope, String keyProperty, Object keyValue) {
        Scope scope = (nullableScope != null) ? nullableScope : GLOBAL_SCOPE;
        ImmutableList<String> tmpScopes = ImmutableList.of(scope.tag, SP_SCOPE.tag, GLOBAL_SCOPE.tag);
        ImmutableList<String> scopes = tmpScopes.subList(tmpScopes.lastIndexOf(scope.tag), tmpScopes.size());
        ImmutableMap.Builder<String, Node> expandsBldr = ImmutableMap.builder();
        graphdb.findNodes(keyLabel, keyProperty, keyValue).forEachRemaining(node -> {
            Iterable<Relationship> relationships = node.getRelationships(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING);
            if (Iterables.size(relationships) != 1) {
                LOGGER.error("Element node {} as too many Planet relations !", node.getProperty(TAG));
            }
            Relationship only = relationships.iterator().next();
            String nodeScope = only.getEndNode().getProperty(_SCOPE).toString();
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
                .orElseGet(() -> UniqueEntity.created(initialize(graphdb.createNode(), keyProperty, keyValue, scope)));

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
        created.setProperty(_SCOPE, scope.tag);
        created.setProperty(GraphModelConstants.CREATED_AT, Instant.now().toEpochMilli());
        return created;
    }

    public static OverridableElementFactory networkElementFactory(GraphDatabaseService graphdb) {
        return new OverridableElementFactory(graphdb, Labels.ELEMENT, Labels.NETWORK_ELEMENT);
    }


}
