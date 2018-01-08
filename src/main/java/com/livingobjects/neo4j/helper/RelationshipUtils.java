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

    public static void addRelationships(Direction direction,
                                        Node startNode,
                                        RelationshipType relationshipType,
                                        UniqueElementFactory uniqueNodeFactory,
                                        Iterable<MatchProperties> nodesToMatch) {
        updateRelationshipsInternal(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, false, node -> {
        });
    }

    public static void addRelationships(Direction direction,
                                        Node startNode,
                                        RelationshipType relationshipType,
                                        Iterable<Node> relationshipsEndNodes) {
        updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, false, node -> {
        });
    }

    public static void updateRelationships(Direction direction,
                                           Node startNode,
                                           RelationshipType relationshipType,
                                           UniqueElementFactory uniqueNodeFactory,
                                           Iterable<MatchProperties> nodesToMatch) {
        replaceRelationships(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, node -> {
        });
    }

    private static void replaceRelationships(Direction direction,
                                             Node startNode,
                                             RelationshipType relationshipType,
                                             UniqueElementFactory uniqueNodeFactory,
                                             Iterable<MatchProperties> nodesToMatch,
                                             Consumer<Node> discardedNodeHandler) {
        updateRelationshipsInternal(direction, startNode, relationshipType, uniqueNodeFactory, nodesToMatch, true, discardedNodeHandler);
    }

    public static void replaceRelationships(Direction direction,
                                            Node startNode,
                                            RelationshipType relationshipType,
                                            Iterable<Node> relationshipsEndNodes,
                                            Consumer<Node> discardedNodeHandler) {
        updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, true, discardedNodeHandler);
    }

    private static void updateRelationshipsInternal(Direction direction,
                                                    Node startNode,
                                                    RelationshipType relationshipType,
                                                    UniqueElementFactory uniqueNodeFactory,
                                                    Iterable<MatchProperties> nodesToMatch,
                                                    boolean removePreviousLinks,
                                                    Consumer<Node> discardedNodeHandler) {
        Set<Node> relationshipsEndNodes = Sets.newLinkedHashSet();
        for (MatchProperties matchProperties : nodesToMatch) {
            UniqueEntity<Node> node = uniqueNodeFactory.getOrCreateWithOutcome(matchProperties);
            relationshipsEndNodes.add(node.entity);
        }
        updateRelationshipsInternal(direction, startNode, relationshipType, relationshipsEndNodes, removePreviousLinks, discardedNodeHandler);
    }

    private static void updateRelationshipsInternal(Direction direction,
                                                    Node startNode,
                                                    RelationshipType relationshipType,
                                                    Iterable<Node> relationshipsEndNodes,
                                                    boolean removePreviousLinks,
                                                    Consumer<Node> discardedNodeHandler) {
        Set<Node> toLinks = Sets.newHashSet(relationshipsEndNodes);
        if (removePreviousLinks) {
            Set<Node> discardedNodes = Sets.newHashSet();
            Iterable<Relationship> relationships = startNode.getRelationships(direction, relationshipType);
            for (Relationship relationship : ImmutableSet.copyOf(relationships)) {
                Node otherNode = relationship.getOtherNode(startNode);
                if (!toLinks.remove(otherNode)) {
                    relationship.delete();
                    discardedNodes.add(otherNode);
                }
            }
            discardedNodes.forEach(discardedNodeHandler);
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
