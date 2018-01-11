package com.livingobjects.neo4j.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.MatchProperties;
import com.livingobjects.neo4j.helper.RelationshipUtils;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.schema.CounterNode;
import com.livingobjects.neo4j.model.schema.MemdexPathNode;
import com.livingobjects.neo4j.model.schema.PlanetNode;
import com.livingobjects.neo4j.model.schema.Schema;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.LINK_PROP_SPECIALIZER;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.PATH;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.SCOPE_SP_TAG;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.VERSION;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._TYPE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.APPLIED_TO;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.EXTEND;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.MEMDEXPATH;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.PROVIDED;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class SchemaLoader {

    private final GraphDatabaseService graphDb;

    private final UniqueElementFactory schemaFactory;

    private final UniqueElementFactory planetTemplateFactory;

    private final UniqueElementFactory realmTemplateFactory;

    private final UniqueElementFactory attributeNodeFactory;

    private final UniqueElementFactory counterNodeFactory;

    public SchemaLoader(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        schemaFactory = new UniqueElementFactory(graphDb, Labels.SCHEMA, Optional.empty());
        planetTemplateFactory = new UniqueElementFactory(graphDb, Labels.PLANET_TEMPLATE, Optional.empty());
        realmTemplateFactory = new UniqueElementFactory(graphDb, Labels.REALM_TEMPLATE, Optional.empty());
        attributeNodeFactory = new UniqueElementFactory(graphDb, Labels.ATTRIBUTE, Optional.empty());
        counterNodeFactory = new UniqueElementFactory(graphDb, Labels.COUNTER, Optional.of(Labels.KPI));
    }

    public boolean load(Schema schema) {
        try (Transaction tx = graphDb.beginTx()) {
            UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schema.id);
            schemaNode.entity.setProperty(VERSION, schema.version);

            Node spScope = graphDb.findNode(Labels.SCOPE, TAG, SCOPE_SP_TAG);
            if (spScope != null) {
                RelationshipUtils.updateRelationships(OUTGOING, schemaNode.entity, APPLIED_TO, ImmutableSet.of(spScope));
            }

            ImmutableMap<String, Node> planets = createPlanets(schema);

            ImmutableSet<Node> realmTemplates = createRealmTemplates(schema, planets, schema.counters);

            ImmutableSet<Node> nodesToLink = ImmutableSet.<Node>builder()
                    .addAll(planets.values())
                    .addAll(realmTemplates)
                    .build();
            RelationshipUtils.replaceRelationships(OUTGOING, schemaNode.entity, RelationshipTypes.PROVIDED, nodesToLink,
                    node -> deleteTree(true, node, OUTGOING, MEMDEXPATH));

            tx.success();
            return schemaNode.wasCreated;
        }
    }

    private ImmutableSet<Node> createRealmTemplates(Schema schema, ImmutableMap<String, Node> planets, ImmutableMap<String, CounterNode> counters) {
        Set<Node> realmTemplates = Sets.newHashSet();
        for (Map.Entry<String, MemdexPathNode> realmNodeEntry : schema.realms.entrySet()) {
            String realmRef = realmNodeEntry.getKey();
            if (!realmRef.startsWith("realm:")) {
                throw new IllegalArgumentException("Malformed realm template ref " + realmRef);
            }
            String realm = realmRef.substring(6);
            UniqueEntity<Node> realmTemplateEntity = realmTemplateFactory.getOrCreateWithOutcome(NAME, realm);

            if (!realmTemplateEntity.wasCreated) {
                deleteTree(false, realmTemplateEntity.entity, OUTGOING, MEMDEXPATH);
            }

            Node memdexPathNode = createMemdexTree(realmNodeEntry.getValue(), planets, counters);

            realmTemplateEntity.entity.createRelationshipTo(memdexPathNode, MEMDEXPATH);

            realmTemplates.add(realmTemplateEntity.entity);
        }
        return ImmutableSet.copyOf(realmTemplates);
    }

    private Node createMemdexTree(MemdexPathNode memdexPath, ImmutableMap<String, Node> planets, ImmutableMap<String, CounterNode> counters) {

        Node memdexPathNode = graphDb.createNode(Labels.SEGMENT);
        memdexPathNode.setProperty(PATH, memdexPath.segment);

        memdexPath.planets.forEach(planetRef -> {
            Node planetNode = planets.get(planetRef);
            if (planetNode == null) {
                throw new IllegalArgumentException(String.format("Planet with reference '%s' is not found in provided schema.", planetRef));
            }
            memdexPathNode.createRelationshipTo(planetNode, EXTEND);
        });

        memdexPath.attributes.forEach(a -> {
            String[] split = a.split(":");
            if (split.length < 2 || split.length > 3) {
                throw new IllegalArgumentException("Malformed attribute " + a);
            }
            String type = split[0];
            String name = split[1];
            String specializer = null;
            if (split.length == 3) {
                specializer = split[2];
            }
            UniqueEntity<Node> attributeNode = attributeNodeFactory.getOrCreateWithOutcome(_TYPE, type, NAME, name);
            Relationship relationshipTo = memdexPathNode.createRelationshipTo(attributeNode.entity, ATTRIBUTE);
            if (specializer == null) {
                relationshipTo.removeProperty(LINK_PROP_SPECIALIZER);
            } else {
                relationshipTo.setProperty(LINK_PROP_SPECIALIZER, specializer);
            }
        });

        for (String counter : memdexPath.counters) {
            CounterNode counterNode = counters.get(counter);
            if (counterNode == null) {
                throw new IllegalArgumentException(String.format("Counter with reference '%s' is not found in provided schema.", counter));
            }
            UniqueEntity<Node> counterNodeEntity = counterNodeFactory.getOrCreateWithOutcome(NAME, counterNode.name);
            if (counterNodeEntity.wasCreated) {
                counterNodeEntity.entity.setProperty("_type", "counter");
                counterNodeEntity.entity.setProperty("defaultAggregation", counterNode.defaultAggregation);
                if (counterNode.defaultValue == null) {
                    counterNodeEntity.entity.removeProperty("defaultValue");
                } else {
                    counterNodeEntity.entity.setProperty("defaultValue", counterNode.defaultValue);
                }
                counterNodeEntity.entity.setProperty("valueType", counterNode.valueType);
                counterNodeEntity.entity.setProperty("unit", counterNode.unit);
            }
            Relationship relationshipTo = counterNodeEntity.entity.createRelationshipTo(memdexPathNode, PROVIDED);
            relationshipTo.setProperty("context", counterNode.context);
        }

        for (MemdexPathNode child : memdexPath.children) {
            Node childNode = createMemdexTree(child, planets, counters);
            memdexPathNode.createRelationshipTo(childNode, MEMDEXPATH);
        }

        return memdexPathNode;
    }

    private void deleteTree(boolean deleteRoot, Node root, Direction direction, RelationshipType relationshipType) {
        Iterable<Relationship> relationships = root.getRelationships(direction, relationshipType);
        for (Relationship relationship : relationships) {
            Node otherNode = relationship.getOtherNode(root);
            deleteTree(true, otherNode, direction, relationshipType);
        }
        if (deleteRoot) {
            root.getRelationships().forEach(Relationship::delete);
            root.delete();
        }
    }

    private ImmutableMap<String, Node> createPlanets(Schema schema) {
        Map<String, Node> planetNodesByName = Maps.newHashMap();
        for (Map.Entry<String, PlanetNode> planetNodeEntry : schema.planets.entrySet()) {
            UniqueEntity<Node> entity = planetTemplateFactory.getOrCreateWithOutcome(NAME, planetNodeEntry.getKey());
            Set<MatchProperties> matchProperties = planetNodeEntry.getValue().attributes
                    .stream()
                    .map(a -> {
                        String[] split = a.split(":");
                        if (split.length != 2) {
                            throw new IllegalArgumentException("Malformed attribute " + a);
                        }
                        return MatchProperties.of(_TYPE, split[0], NAME, split[1]);
                    })
                    .collect(Collectors.toSet());
            RelationshipUtils.replaceRelationships(OUTGOING, entity.entity, ATTRIBUTE, attributeNodeFactory, matchProperties);
            planetNodesByName.put(planetNodeEntry.getKey(), entity.entity);
        }
        return ImmutableMap.copyOf(planetNodesByName);
    }

}
