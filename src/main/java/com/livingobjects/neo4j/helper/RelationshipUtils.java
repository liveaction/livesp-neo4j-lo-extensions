package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.Set;
import java.util.function.Consumer;

public class RelationshipUtils {

    private RelationshipUtils() {
    }

    /**
     * Update the given relationships between the given nodes (create or update it).
     * Other relationships of the same type that already exists are not removed.
     *
     * @return true if database has been modified, false otherwise.
     */
    public static boolean updateRelationships(Direction direction,
                                              Node startNode,
                                              RelationshipType relationshipType,
                                              UniqueElementFactory uniqueNodeFactory,
                                              Iterable<MatchProperties> nodesToMatch,
                                              Transaction tx) {
        return updateRelationshipsInternal(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, false, node -> {
        }, tx);
    }

    /**
     * Update the given relationships between the given nodes (create or update it).
     * Other relationships of the same type that already exists are not removed.
     *
     * @return true if database has been modified, false otherwise.
     */
    public static boolean updateRelationships(Direction direction,
                                              Node startNode,
                                              RelationshipType relationshipType,
                                              Iterable<Node> relationshipsEndNodes) {
        return updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, false, node -> {
        });
    }

    /**
     * Update the given relationships between the given nodes.
     * Other relationships of the same type that already exists are removed.
     *
     * @return true if database has been modified, false otherwise.
     */
    public static boolean replaceRelationships(Direction direction,
                                               Node startNode,
                                               RelationshipType relationshipType,
                                               UniqueElementFactory uniqueNodeFactory,
                                               Iterable<MatchProperties> nodesToMatch,
                                               Transaction tx) {
        return replaceRelationships(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, node -> {
        }, tx);
    }

    /**
     * Update the given relationships between the given nodes.
     * Other relationships of the same type that already exists are removed.
     *
     * @return true if database has been modified, false otherwise.
     */
    public static boolean replaceRelationships(Direction direction,
                                               Node startNode,
                                               RelationshipType relationshipType,
                                               Iterable<Node> relationshipsEndNodes) {
        return replaceRelationships(direction, startNode, relationshipType, relationshipsEndNodes, node -> {
        });
    }

    /**
     * Update the given relationships between the given nodes.
     * Other relationships of the same type that already exists are removed.
     * Can prodive a handler to handle nodes that have been detached during process.
     *
     * @return true if database has been modified, false otherwise.
     */
    public static boolean replaceRelationships(Direction direction,
                                               Node startNode,
                                               RelationshipType relationshipType,
                                               Iterable<Node> relationshipsEndNodes,
                                               Consumer<Node> detachedNodeHandler) {
        return updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, true, detachedNodeHandler);
    }

    private static boolean replaceRelationships(Direction direction,
                                                Node startNode,
                                                RelationshipType relationshipType,
                                                UniqueElementFactory uniqueNodeFactory,
                                                Iterable<MatchProperties> nodesToMatch,
                                                Consumer<Node> detachedNodeHandler,
                                                Transaction tx) {
        return updateRelationshipsInternal(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, true, detachedNodeHandler, tx);
    }

    private static boolean updateRelationshipsInternal(Direction direction,
                                                       Node startNode,
                                                       RelationshipType relationshipType,
                                                       UniqueElementFactory uniqueNodeFactory,
                                                       Iterable<MatchProperties> nodesToMatch,
                                                       boolean removeOtherRelationships,
                                                       Consumer<Node> detachedNodeHandler,
                                                       Transaction tx) {
        Set<Node> relationshipsEndNodes = Sets.newLinkedHashSet();
        for (MatchProperties matchProperties : nodesToMatch) {
            UniqueEntity<Node> node = uniqueNodeFactory.getOrCreateWithOutcome(matchProperties, tx);
            relationshipsEndNodes.add(node.entity);
        }
        return updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, removeOtherRelationships, detachedNodeHandler);
    }

    private static boolean updateRelationshipsInternal(Direction direction,
                                                       Node startNode,
                                                       RelationshipType relationshipType,
                                                       Iterable<Node> relationshipsEndNodes,
                                                       boolean removeOtherRelationships,
                                                       Consumer<Node> detachedNodeHandler) {
        boolean modified = false;
        Set<Node> toLinks = Sets.newHashSet(relationshipsEndNodes);

        Set<Node> detachedNodes = Sets.newHashSet();
        Iterable<Relationship> relationships = startNode.getRelationships(direction, relationshipType);
        for (Relationship relationship : ImmutableSet.copyOf(relationships)) {
            Node otherNode = relationship.getOtherNode(startNode);
            if (!toLinks.remove(otherNode)) {
                if (removeOtherRelationships) {
                    relationship.delete();
                    modified = true;
                    detachedNodes.add(otherNode);
                }
            }
        }
        detachedNodes.forEach(detachedNodeHandler);
        for (Node relationshipsEndNode : toLinks) {
            if (direction == Direction.OUTGOING) {
                startNode.createRelationshipTo(relationshipsEndNode, relationshipType);
            } else {
                relationshipsEndNode.createRelationshipTo(startNode, relationshipType);
            }
            modified = true;
        }
        return modified;
    }

}
