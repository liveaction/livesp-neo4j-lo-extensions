package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.Relationship;
import com.livingobjects.neo4j.model.iwan.RelationshipStatus;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

public final class TopologyLoader {

    private final GraphDatabaseService graphDb;

    private final UniqueElementFactory networkElementFactory;

    private final ImmutableMap<String, ImmutableSet<String>> crossAttributeRelationships;

    public TopologyLoader(GraphDatabaseService graphDb) {
        try (Transaction ignore = graphDb.beginTx()) {
            this.networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);

            Map<String, Set<String>> rels = Maps.newHashMap();
            graphDb.findNodes(Labels.ATTRIBUTE)
                    .forEachRemaining(a -> {
                        for (org.neo4j.graphdb.Relationship relationship : a.getRelationships(RelationshipTypes.CROSS_ATTRIBUTE, Direction.OUTGOING)) {
                            Node startNode = relationship.getStartNode();
                            Node endNode = relationship.getEndNode();
                            String from = startNode.getProperty(GraphModelConstants._TYPE).toString() + ':' + startNode.getProperty(GraphModelConstants.NAME).toString();
                            String to = endNode.getProperty(GraphModelConstants._TYPE).toString() + ':' + endNode.getProperty(GraphModelConstants.NAME).toString();
                            rels.computeIfAbsent(from, k -> Sets.newHashSet()).add(to);
                        }
                    });
            this.crossAttributeRelationships = ImmutableMap.copyOf(Maps.transformValues(rels, ImmutableSet::copyOf));
        }
        this.graphDb = graphDb;
    }

    public void loadRelationships(List<Relationship> relationships, Consumer<RelationshipStatus> relationshipStatusConsumer) {
        try (Transaction tx = graphDb.beginTx()) {
            List<RelationshipStatus> statuses = Lists.newArrayList();
            for (Relationship relationship : relationships) {
                try {
                    loadRelationship(relationship);
                    statuses.add(new RelationshipStatus(relationship.type, relationship.from, relationship.to, true, null));
                } catch (Throwable e) {
                    statuses.add(new RelationshipStatus(relationship.type, relationship.from, relationship.to, false, e.getMessage()));
                }
            }
            tx.success();
            statuses.forEach(relationshipStatusConsumer);
        } catch (Throwable e) {
            String message = e.getMessage();
            relationships.forEach(relationship -> relationshipStatusConsumer.accept(new RelationshipStatus(relationship.type, relationship.from, relationship.to, false, message)));
        }
    }

    private void loadRelationship(Relationship relationship) {
        ImportRelationship relationshipType = ImportRelationship.of(relationship.type);
        if (relationshipType != null) {
            if (relationship.from.equals(relationship.to)) {
                throw new IllegalArgumentException("From and to elements must be different");
            }
            Node from = networkElementFactory.getWithOutcome(GraphModelConstants.TAG, relationship.from);
            if (from == null) {
                throw new IllegalArgumentException(String.format("Start element '%s' not found", relationship.from));
            }
            Node to = networkElementFactory.getWithOutcome(GraphModelConstants.TAG, relationship.to);
            if (to == null) {
                throw new IllegalArgumentException(String.format("Target element '%s' not found", relationship.to));
            }
            String fromType = from.getProperty(GraphModelConstants._TYPE).toString();
            String toType = to.getProperty(GraphModelConstants._TYPE).toString();
            ImmutableSet<String> authorizedRels = crossAttributeRelationships.get(fromType);
            if (authorizedRels != null && authorizedRels.contains(toType)) {
                org.neo4j.graphdb.Relationship r = mergeRelationship(from, to, relationshipType);
                for (Map.Entry<String, Object> e : relationship.attributes.entrySet()) {
                    r.setProperty(e.getKey(), e.getValue());
                }
                relationship.attributes.forEach((key, value) -> {
                    if (value != null) {
                        r.setProperty(key, value);
                    } else {
                        r.removeProperty(key);
                    }
                });
            } else {
                throw new IllegalArgumentException(String.format("Relationship %s is not allowed from '%s' to '%s'", relationship.type, fromType, toType));
            }
        } else {
            throw new IllegalArgumentException(String.format("Relationship type %s is not allowed", relationship.type));
        }
    }

    private org.neo4j.graphdb.Relationship mergeRelationship(Node from, Node to, ImportRelationship relationshipType) {
        org.neo4j.graphdb.Relationship existingRelationship = null;
        for (org.neo4j.graphdb.Relationship r : from.getRelationships(relationshipType.relationshipType, Direction.OUTGOING)) {
            if (r.getEndNode().equals(to)) {
                existingRelationship = r;
            }
        }
        if (existingRelationship == null) {
            existingRelationship = from.createRelationshipTo(to, relationshipType.relationshipType);
        }
        return existingRelationship;
    }

}
