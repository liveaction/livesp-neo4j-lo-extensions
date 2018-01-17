package com.livingobjects.neo4j.schema;

import com.google.common.base.Throwables;
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
import com.livingobjects.neo4j.model.schema.PartialSchema;
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
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

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
import static java.util.stream.Collectors.toSet;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class SchemaLoader {

    private static final ReentrantLock schemaLock = new ReentrantLock();

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

    public boolean updateRealmPath(String schemaId, String realmTemplate, PartialSchema partialSchema) {
        return lockAndWriteSchema(() -> {
            UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schemaId);
            if (schemaNode.wasCreated) {
                schemaNode.entity.setProperty(VERSION, "1.0");
                Node spScope = graphDb.findNode(Labels.SCOPE, TAG, SCOPE_SP_TAG);
                if (spScope != null) {
                    RelationshipUtils.updateRelationships(INCOMING, schemaNode.entity, APPLIED_TO, ImmutableSet.of(spScope));
                }
            }

            UniqueEntity<Node> realmTemplateEntity = realmTemplateFactory.getOrCreateWithOutcome(NAME, realmTemplate);
            Iterable<Relationship> relationships = realmTemplateEntity.entity.getRelationships(INCOMING, PROVIDED);
            for (Relationship relationship : relationships) {
                Node schema = relationship.getStartNode();
                String existingSchemaId = schema.getProperty(ID, "").toString();
                if (!existingSchemaId.equalsIgnoreCase(schemaId)) {
                    throw new IllegalArgumentException(String.format("Realm '%s' is already provided by schema '%s'", realmTemplate, existingSchemaId));
                }
            }

            Set<Node> collectedPlanets = Sets.newHashSet();
            Function<String, Node> getAndCollectPlanets = (planet) -> {
                Node node = planetTemplateFactory.getWithOutcome(NAME, planet);
                collectedPlanets.add(node);
                return node;
            };

            Relationship firstLevel = realmTemplateEntity.entity.getSingleRelationship(MEMDEXPATH, OUTGOING);
            if (firstLevel == null) {
                Node memdexTree = createMemdexTree(partialSchema.path, getAndCollectPlanets, partialSchema.counters);
                realmTemplateEntity.entity.createRelationshipTo(memdexTree, MEMDEXPATH);
            } else {
                Node firstSegment = firstLevel.getEndNode();

                Object existingPath = firstSegment.getProperty(PATH);

                if (existingPath.equals(partialSchema.path.segment)) {
                    updateMemdexTree(partialSchema.path, getAndCollectPlanets, partialSchema.counters, firstSegment);
                } else {
                    throw new IllegalArgumentException(String.format("Realm %s can have only one root level : %s != %s", realmTemplate, existingPath, partialSchema.path.segment));
                }
            }

            RelationshipUtils.updateRelationships(OUTGOING, schemaNode.entity, PROVIDED, collectedPlanets);

            if (realmTemplateEntity.wasCreated) {
                for (Relationship relationship : realmTemplateEntity.entity.getRelationships(PROVIDED, INCOMING)) {
                    relationship.delete();
                }
                schemaNode.entity.createRelationshipTo(realmTemplateEntity.entity, PROVIDED);
            }
            return schemaNode.wasCreated;
        });
    }

    public boolean load(Schema schema) {
        return lockAndWriteSchema(() -> {
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

            return schemaNode.wasCreated;
        });
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

            Node memdexPathNode = createMemdexTree(realmNodeEntry.getValue(), planets::get, counters);

            realmTemplateEntity.entity.createRelationshipTo(memdexPathNode, MEMDEXPATH);

            realmTemplates.add(realmTemplateEntity.entity);
        }
        return ImmutableSet.copyOf(realmTemplates);
    }

    private void mergeMemdexTree(Node parentNode, MemdexPathNode memdexPath, Function<String, Node> planets, ImmutableMap<String, CounterNode> counters) {
        Node segmentNode = null;
        for (Relationship relationship : parentNode.getRelationships(OUTGOING, MEMDEXPATH)) {
            Node childNode = relationship.getEndNode();
            String path = childNode.getProperty(PATH, "").toString();
            if (memdexPath.segment.equals(path)) {
                segmentNode = childNode;
                break;
            }
        }
        if (segmentNode == null) {
            segmentNode = graphDb.createNode(Labels.SEGMENT);
            segmentNode.setProperty(PATH, memdexPath.segment);
            parentNode.createRelationshipTo(segmentNode, MEMDEXPATH);
        }
        updateMemdexTree(memdexPath, planets, counters, segmentNode);
    }

    private void updateMemdexTree(MemdexPathNode memdexPath, Function<String, Node> planets, ImmutableMap<String, CounterNode> counters, Node segmentNode) {
        replacePlanetTemplatesRelationships(memdexPath, planets, segmentNode);

        replaceAttributesRelationships(memdexPath, segmentNode);

        updateCountersRelationships(memdexPath, counters, segmentNode);

        for (MemdexPathNode child : memdexPath.children) {
            mergeMemdexTree(segmentNode, child, planets, counters);
        }
    }

    private Node createMemdexTree(MemdexPathNode memdexPath, Function<String, Node> planets, ImmutableMap<String, CounterNode> counters) {

        Node segmentNode = graphDb.createNode(Labels.SEGMENT);
        segmentNode.setProperty(PATH, memdexPath.segment);

        replacePlanetTemplatesRelationships(memdexPath, planets, segmentNode);

        replaceAttributesRelationships(memdexPath, segmentNode);

        updateCountersRelationships(memdexPath, counters, segmentNode);

        for (MemdexPathNode child : memdexPath.children) {
            Node childNode = createMemdexTree(child, planets, counters);
            segmentNode.createRelationshipTo(childNode, MEMDEXPATH);
        }

        return segmentNode;
    }

    private void updateCountersRelationships(MemdexPathNode memdexPath, ImmutableMap<String, CounterNode> counters, Node segmentNode) {
        Map<String, Relationship> existingRelationships = Maps.newHashMap();
        for (Relationship relationship : segmentNode.getRelationships(INCOMING, PROVIDED)) {
            Node counterNode = relationship.getStartNode();
            String name = counterNode.getProperty(NAME).toString();
            existingRelationships.put(name, relationship);
        }

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

            Relationship relationshipTo = existingRelationships.get(counterNode.name);
            if (relationshipTo == null) {
                relationshipTo = counterNodeEntity.entity.createRelationshipTo(segmentNode, PROVIDED);
            }
            relationshipTo.setProperty("context", counterNode.context);
        }
    }

    private void replaceAttributesRelationships(MemdexPathNode memdexPath, Node segmentNode) {
        segmentNode.getRelationships(OUTGOING, ATTRIBUTE).forEach(Relationship::delete);
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
            Relationship relationshipTo = segmentNode.createRelationshipTo(attributeNode.entity, ATTRIBUTE);
            if (specializer == null) {
                relationshipTo.removeProperty(LINK_PROP_SPECIALIZER);
            } else {
                relationshipTo.setProperty(LINK_PROP_SPECIALIZER, specializer);
            }
        });
    }

    private void replacePlanetTemplatesRelationships(MemdexPathNode memdexPath, Function<String, Node> planets, Node segmentNode) {
        Set<Node> planetNodes = memdexPath.planets.stream()
                .map(planetRef -> {
                    Node planetNode = planets.apply(planetRef);
                    if (planetNode == null) {
                        throw new IllegalArgumentException(String.format("Planet with reference '%s' is not found in provided schema.", planetRef));
                    }
                    return planetNode;
                })
                .collect(toSet());
        RelationshipUtils.replaceRelationships(OUTGOING, segmentNode, EXTEND, planetNodes);
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
                    .collect(toSet());
            RelationshipUtils.replaceRelationships(OUTGOING, entity.entity, ATTRIBUTE, attributeNodeFactory, matchProperties);
            planetNodesByName.put(planetNodeEntry.getKey(), entity.entity);
        }
        return ImmutableMap.copyOf(planetNodesByName);
    }

    private <T> T lockAndWriteSchema(Callable<T> schemaModification) {
        try {
            schemaLock.lock();
            try (Transaction tx = graphDb.beginTx()) {
                T result = schemaModification.call();
                tx.success();
                return result;
            }
        } catch (Throwable e) {
            throw Throwables.propagate(e);
        } finally {
            schemaLock.unlock();
        }
    }

}
