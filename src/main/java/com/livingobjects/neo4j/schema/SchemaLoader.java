package com.livingobjects.neo4j.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.MatchProperties;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.schema.PlanetNode;
import com.livingobjects.neo4j.model.schema.Schema;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

public final class SchemaLoader {

    private final GraphDatabaseService graphDb;

    private final UniqueElementFactory schemaFactory;

    private final UniqueElementFactory planetTemplateFactory;

    private final UniqueElementFactory attributeNodeFactory;

    public SchemaLoader(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        schemaFactory = new UniqueElementFactory(graphDb, Labels.SCHEMA, Optional.empty());
        planetTemplateFactory = new UniqueElementFactory(graphDb, Labels.PLANET_TEMPLATE, Optional.empty());
        attributeNodeFactory = new UniqueElementFactory(graphDb, Labels.ATTRIBUTE, Optional.empty());
    }

    public boolean load(Schema schema) {
        try (Transaction tx = graphDb.beginTx()) {
            UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schema.id);

            ImmutableMap<String, Node> planetNodes = createPlanets(schema);

            updateRelationships(Direction.OUTGOING, schemaNode.entity, RelationshipTypes.PROVIDED, planetNodes.values());

            tx.success();
            return schemaNode.wasCreated;
        }
    }

    private ImmutableMap<String, Node> createPlanets(Schema schema) {
        Map<String, Node> planetNodesByName = Maps.newHashMap();
        for (Map.Entry<String, PlanetNode> planetNodeEntry : schema.planets.entrySet()) {
            UniqueEntity<Node> entity = planetTemplateFactory.getOrCreateWithOutcome(NAME, planetNodeEntry.getKey());
            updateRelationships(Direction.OUTGOING, entity.entity, RelationshipTypes.ATTRIBUTE, attributeNodeFactory,
                    planetNodeEntry.getValue().attributes.stream()
                            .map(a -> {
                                String[] split = a.split(":");
                                if (split.length != 2) {
                                    throw new IllegalArgumentException("Malformed attribute " + a);
                                }
                                return MatchProperties.of(_TYPE, split[0], NAME, split[1]);
                            })
                            .collect(Collectors.toSet()));
            planetNodesByName.put(planetNodeEntry.getKey(), entity.entity);
        }
        return ImmutableMap.copyOf(planetNodesByName);
    }

    private void updateRelationships(Direction direction,
                                     Node startNode,
                                     RelationshipType relationshipType,
                                     UniqueElementFactory uniqueNodeFactory,
                                     Iterable<MatchProperties> nodesToMatch) {
        Set<Node> relationshipsEndNodes = Sets.newLinkedHashSet();
        for (MatchProperties matchProperties : nodesToMatch) {
            UniqueEntity<Node> node = uniqueNodeFactory.getOrCreateWithOutcome(matchProperties);
            relationshipsEndNodes.add(node.entity);
        }
        updateRelationships(direction, startNode, relationshipType, relationshipsEndNodes);
    }

    private void updateRelationships(Direction direction,
                                     Node startNode,
                                     RelationshipType relationshipType,
                                     Iterable<Node> relationshipsEndNodes) {
        HashSet<Node> toLinks = Sets.newHashSet(relationshipsEndNodes);
        Iterable<Relationship> relationships = startNode.getRelationships(direction, relationshipType);
        for (Relationship relationship : relationships) {
            if (!toLinks.remove(relationship.getOtherNode(startNode))) {
                relationship.delete();
            }
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
