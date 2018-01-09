package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.Set;
import java.util.function.Consumer;

public class RelationshipUtils {

    private RelationshipUtils() {
    }

    /**
     * Update the given relationships between the given nodes (create or update it).
     * Other relationships of the same type that already exists are not removed.
     */
    public static void updateRelationships(Direction direction,
                                           Node startNode,
                                           RelationshipType relationshipType,
                                           UniqueElementFactory uniqueNodeFactory,
                                           Iterable<MatchProperties> nodesToMatch) {
        updateRelationshipsInternal(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, false, node -> {
        });
    }

    /**
     * Update the given relationships between the given nodes (create or update it).
     * Other relationships of the same type that already exists are not removed.
     */
    public static void updateRelationships(Direction direction,
                                           Node startNode,
                                           RelationshipType relationshipType,
                                           Iterable<Node> relationshipsEndNodes) {
        updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, false, node -> {
        });
    }

    /**
     * Update the given relationships between the given nodes.
     * Other relationships of the same type that already exists are removed.
     */
    public static void replaceRelationships(Direction direction,
                                            Node startNode,
                                            RelationshipType relationshipType,
                                            UniqueElementFactory uniqueNodeFactory,
                                            Iterable<MatchProperties> nodesToMatch) {
        replaceRelationships(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, node -> {
        });
    }

    /**
     * Update the given relationships between the given nodes.
     * Other relationships of the same type that already exists are removed.
     * Can prodive a handler to handle nodes that have been detached during process.
     */
    public static void replaceRelationships(Direction direction,
                                            Node startNode,
                                            RelationshipType relationshipType,
                                            Iterable<Node> relationshipsEndNodes,
                                            Consumer<Node> detachedNodeHandler) {
        updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, true, detachedNodeHandler);
    }

    private static void replaceRelationships(Direction direction,
                                             Node startNode,
                                             RelationshipType relationshipType,
                                             UniqueElementFactory uniqueNodeFactory,
                                             Iterable<MatchProperties> nodesToMatch,
                                             Consumer<Node> detachedNodeHandler) {
        updateRelationshipsInternal(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, true, detachedNodeHandler);
    }

    private static void updateRelationshipsInternal(Direction direction,
                                                    Node startNode,
                                                    RelationshipType relationshipType,
                                                    UniqueElementFactory uniqueNodeFactory,
                                                    Iterable<MatchProperties> nodesToMatch,
                                                    boolean removeOtherRelationships,
                                                    Consumer<Node> detachedNodeHandler) {
        Set<Node> relationshipsEndNodes = Sets.newLinkedHashSet();
        for (MatchProperties matchProperties : nodesToMatch) {
            UniqueEntity<Node> node = uniqueNodeFactory.getOrCreateWithOutcome(matchProperties);
            relationshipsEndNodes.add(node.entity);
        }
        updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, removeOtherRelationships, detachedNodeHandler);
    }

    private static void updateRelationshipsInternal(Direction direction,
                                                    Node startNode,
                                                    RelationshipType relationshipType,
                                                    Iterable<Node> relationshipsEndNodes,
                                                    boolean removeOtherRelationships,
                                                    Consumer<Node> detachedNodeHandler) {
        Set<Node> toLinks = Sets.newHashSet(relationshipsEndNodes);
        if (removeOtherRelationships) {
            Set<Node> detachedNodes = Sets.newHashSet();
            Iterable<Relationship> relationships = startNode.getRelationships(direction, relationshipType);
            for (Relationship relationship : ImmutableSet.copyOf(relationships)) {
                Node otherNode = relationship.getOtherNode(startNode);
                if (!toLinks.remove(otherNode)) {
                    relationship.delete();
                    detachedNodes.add(otherNode);
                }
            }
            detachedNodes.forEach(detachedNodeHandler);
        }
        for (Node relationshipsEndNode : toLinks) {
            if (direction == Direction.OUTGOING) {
                startNode.createRelationshipTo(relationshipsEndNode, relationshipType);
            } else {
                relationshipsEndNode.createRelationshipTo(startNode, relationshipType);
            }
        }
    }

}
