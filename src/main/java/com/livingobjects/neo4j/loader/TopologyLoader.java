package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.PropertyConverter;
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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

public final class TopologyLoader {

    private final GraphDatabaseService graphDb;

    private final UniqueElementFactory networkElementFactory;

    private final ImmutableMap<String, ImmutableSet<String>> crossAttributeRelationships;

    private final TopologyLoaderUtils topologyLoaderUtils;

    public TopologyLoader(GraphDatabaseService graphDb) {
        try (Transaction tx = graphDb.beginTx()) {
            this.networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);

            Map<String, Set<String>> rels = Maps.newHashMap();
            tx.findNodes(Labels.ATTRIBUTE)
                    .forEachRemaining(a -> {
                        for (org.neo4j.graphdb.Relationship relationship : a.getRelationships(Direction.OUTGOING, RelationshipTypes.CROSS_ATTRIBUTE)) {
                            Node startNode = relationship.getStartNode();
                            Node endNode = relationship.getEndNode();
                            String from = startNode.getProperty(GraphModelConstants._TYPE).toString() + ':' + startNode.getProperty(GraphModelConstants.NAME).toString();
                            String to = endNode.getProperty(GraphModelConstants._TYPE).toString() + ':' + endNode.getProperty(GraphModelConstants.NAME).toString();
                            rels.computeIfAbsent(from, k -> Sets.newHashSet()).add(to);
                        }
                    });
            this.crossAttributeRelationships = ImmutableMap.copyOf(Maps.transformValues(rels, ImmutableSet::copyOf));

            UniqueElementFactory scopeElementFactory = new UniqueElementFactory(graphDb, Labels.SCOPE, Optional.empty());

            this.topologyLoaderUtils = new TopologyLoaderUtils(scopeElementFactory);
        }

        this.graphDb = graphDb;
    }

    public void loadRelationships(List<Relationship> relationships, Consumer<RelationshipStatus> relationshipStatusConsumer, boolean updateOnly) {
        try (Transaction tx = graphDb.beginTx()) {
            List<RelationshipStatus> statuses = Lists.newArrayList();
            for (Relationship relationship : relationships) {
                try {
                    loadRelationship(relationship, updateOnly, tx);
                    statuses.add(new RelationshipStatus(relationship.type, relationship.from, relationship.to, true, null));
                } catch (Throwable e) {
                    statuses.add(new RelationshipStatus(relationship.type, relationship.from, relationship.to, false, e.getMessage()));
                }
            }
            tx.commit();
            statuses.forEach(relationshipStatusConsumer);
        } catch (Throwable e) {
            String message = e.getMessage();
            relationships.forEach(relationship -> relationshipStatusConsumer.accept(new RelationshipStatus(relationship.type, relationship.from, relationship.to, false, message)));
        }
    }

    private void loadRelationship(Relationship relationship, boolean updateOnly, Transaction tx) {
        ImportRelationship relationshipType = ImportRelationship.of(relationship.type);
        if (relationshipType != null) {
            if (relationship.from.equals(relationship.to)) {
                throw new IllegalArgumentException("From and to elements must be different");
            }
            Node from = networkElementFactory.getWithOutcome(GraphModelConstants.TAG, relationship.from, tx);
            if (from == null) {
                throw new IllegalArgumentException(String.format("Start element '%s' not found", relationship.from));
            }
            Node to = networkElementFactory.getWithOutcome(GraphModelConstants.TAG, relationship.to, tx);
            if (to == null) {
                throw new IllegalArgumentException(String.format("Target element '%s' not found", relationship.to));
            }
            String fromType = from.getProperty(GraphModelConstants._TYPE).toString();
            String toType = to.getProperty(GraphModelConstants._TYPE).toString();

            ImmutableSet<String> authorizedRels = crossAttributeRelationships.get(fromType);

            if (authorizedRels != null && authorizedRels.contains(toType)) {
                if (nodeScopesAreEqualOrGlobal(from, to, tx)) {
                    org.neo4j.graphdb.Relationship r = mergeRelationship(from, to, relationshipType, updateOnly);

                    for (Map.Entry<String, Object> e : relationship.attributes.entrySet()) {
                        r.setProperty(e.getKey(), PropertyConverter.checkPropertyValue(e.getValue()));
                    }

                    relationship.attributes.forEach((key, value) -> {
                        Object checkedValue = PropertyConverter.checkPropertyValue(value);
                        if (checkedValue != null) {
                            r.setProperty(key, checkedValue);
                        } else {
                            r.removeProperty(key);
                        }
                    });

                    r.setProperty(GraphModelConstants.UPDATED_AT, Instant.now().toEpochMilli());
                } else {
                    throw new IllegalArgumentException(
                            String.format("Relationship %s is not allowed from '%s' to '%s' : elements should have the same scope or one element should have the global scope",
                                    relationship.type, fromType, toType));
                }
            } else {
                throw new IllegalArgumentException(String.format("Relationship %s is not allowed from '%s' to '%s'", relationship.type, fromType, toType));
            }
        } else {
            throw new IllegalArgumentException(String.format("Relationship type %s is not allowed", relationship.type));
        }
    }

    private boolean nodeScopesAreEqualOrGlobal(Node nodeLeft, Node nodeRight, Transaction tx) {
        Scope scopeLeft = topologyLoaderUtils.getScope(nodeLeft, tx);
        Scope scopeRight = topologyLoaderUtils.getScope(nodeRight, tx);

        boolean scopeAreEqual = scopeLeft.equals(scopeRight);
        boolean oneScopeIsGlobal = scopeLeft.equals(GraphModelConstants.GLOBAL_SCOPE) || scopeRight.equals(GraphModelConstants.GLOBAL_SCOPE);

        return scopeAreEqual || oneScopeIsGlobal;
    }

    private org.neo4j.graphdb.Relationship mergeRelationship(Node from, Node to, ImportRelationship relationshipType, boolean updateOnly) {
        org.neo4j.graphdb.Relationship existingRelationship = null;

        for (org.neo4j.graphdb.Relationship r : from.getRelationships(Direction.OUTGOING, relationshipType.relationshipType)) {
            if (r.getEndNode().equals(to)) {
                existingRelationship = r;
            }
        }

        if (existingRelationship == null) {
            if (updateOnly) {
                throw new IllegalArgumentException("Unable to update the relationship : it must be created first.");
            }

            existingRelationship = from.createRelationshipTo(to, relationshipType.relationshipType);
            existingRelationship.setProperty(GraphModelConstants.CREATED_AT, Instant.now().toEpochMilli());
        }

        return existingRelationship;
    }

}
