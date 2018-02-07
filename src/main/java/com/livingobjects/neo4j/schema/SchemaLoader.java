package com.livingobjects.neo4j.schema;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.MatchProperties;
import com.livingobjects.neo4j.helper.PlanetByContext;
import com.livingobjects.neo4j.helper.RelationshipUtils;
import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.schema.CounterNode;
import com.livingobjects.neo4j.model.schema.MemdexPathNode;
import com.livingobjects.neo4j.model.schema.PartialSchema;
import com.livingobjects.neo4j.model.schema.PlanetNode;
import com.livingobjects.neo4j.model.schema.PlanetUpdate;
import com.livingobjects.neo4j.model.schema.RealmNode;
import com.livingobjects.neo4j.model.schema.Schema;
import com.livingobjects.neo4j.model.schema.SchemaAndPlanets;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
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
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.MEMDEXPATH;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.PROVIDED;
import static com.livingobjects.neo4j.model.schema.PlanetUpdateStatus.DELETE;
import static com.livingobjects.neo4j.model.schema.PlanetUpdateStatus.UPDATE;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;

public final class SchemaLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(SchemaLoader.class);

    private static final ReentrantLock schemaLock = new ReentrantLock();

    private final GraphDatabaseService graphDb;

    private final UniqueElementFactory schemaFactory;

    private final UniqueElementFactory planetTemplateFactory;

    private final UniqueElementFactory realmTemplateFactory;

    private final UniqueElementFactory attributeNodeFactory;

    private final UniqueElementFactory counterNodeFactory;


    private final UniqueElementFactory planetFactory;

    public SchemaLoader(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        schemaFactory = new UniqueElementFactory(graphDb, Labels.SCHEMA, Optional.empty());
        planetTemplateFactory = new UniqueElementFactory(graphDb, Labels.PLANET_TEMPLATE, Optional.empty());
        realmTemplateFactory = new UniqueElementFactory(graphDb, Labels.REALM_TEMPLATE, Optional.empty());
        attributeNodeFactory = new UniqueElementFactory(graphDb, Labels.ATTRIBUTE, Optional.empty());
        counterNodeFactory = new UniqueElementFactory(graphDb, Labels.COUNTER, Optional.of(Labels.KPI));
        this.planetFactory = UniqueElementFactory.planetFactory(graphDb);
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
            for (Relationship relationship : realmTemplateEntity.entity.getRelationships(ATTRIBUTE, OUTGOING)) {
                relationship.delete();
            }
            for (String attribute : partialSchema.attributes) {
                String[] split = attribute.split(":");
                if (split.length < 2 || split.length > 3) {
                    throw new IllegalArgumentException("Malformed attribute " + attribute);
                }
                String type = split[0];
                String name = split[1];
                UniqueEntity<Node> attributeNode = attributeNodeFactory.getOrCreateWithOutcome(_TYPE, type, NAME, name);
                realmTemplateEntity.entity.createRelationshipTo(attributeNode.entity, RelationshipTypes.ATTRIBUTE);
            }
            Iterable<Relationship> relationships = realmTemplateEntity.entity.getRelationships(INCOMING, PROVIDED);
            for (Relationship relationship : relationships) {
                Node schema = relationship.getStartNode();
                String existingSchemaId = schema.getProperty(ID, "").toString();
                if (!existingSchemaId.equalsIgnoreCase(schemaId)) {
                    throw new IllegalArgumentException(String.format("Realm '%s' is already provided by schema '%s'", realmTemplate, existingSchemaId));
                }
            }

            Relationship firstLevel = realmTemplateEntity.entity.getSingleRelationship(MEMDEXPATH, OUTGOING);
            if (firstLevel == null) {
                Node memdexTree = createMemdexTree(partialSchema.memdexPath, partialSchema.counters);
                realmTemplateEntity.entity.createRelationshipTo(memdexTree, MEMDEXPATH);
            } else {
                Node firstSegment = firstLevel.getEndNode();

                Object existingPath = firstSegment.getProperty(PATH);

                if (existingPath.equals(partialSchema.memdexPath.segment)) {
                    updateMemdexTree(partialSchema.memdexPath, partialSchema.counters, firstSegment);
                } else {
                    throw new IllegalArgumentException(String.format("Realm %s can have only one root level : %s != %s", realmTemplate, existingPath, partialSchema.memdexPath.segment));
                }
            }

            if (realmTemplateEntity.wasCreated) {
                for (Relationship relationship : realmTemplateEntity.entity.getRelationships(PROVIDED, INCOMING)) {
                    relationship.delete();
                }
                schemaNode.entity.createRelationshipTo(realmTemplateEntity.entity, PROVIDED);
            }
            return schemaNode.wasCreated;
        });
    }

    public boolean load(SchemaAndPlanets schemaAndPlanets) {
        return lockAndWriteSchema(() -> {
            Schema schema = schemaAndPlanets.schema;
            ImmutableList<PlanetUpdate> planetUpdate = schemaAndPlanets.planetMigrations;
            UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schema.id);
            schemaNode.entity.setProperty(VERSION, schema.version);

            Node spScope = graphDb.findNode(Labels.SCOPE, TAG, SCOPE_SP_TAG);
            if (spScope != null) {
                RelationshipUtils.updateRelationships(OUTGOING, schemaNode.entity, APPLIED_TO, ImmutableSet.of(spScope));
            }

            ImmutableSet<Node> realmTemplates = createRealmTemplates(schema, schema.counters);

            ImmutableSet<Node> nodesToLink = ImmutableSet.<Node>builder()
                    .addAll(realmTemplates)
                    .build();
            RelationshipUtils.replaceRelationships(OUTGOING, schemaNode.entity, RelationshipTypes.PROVIDED, nodesToLink,
                    node -> deleteTree(true, node, OUTGOING, MEMDEXPATH));

            migratePlanets(planetUpdate);

            return schemaNode.wasCreated;
        });
    }

    private void migratePlanets(ImmutableList<PlanetUpdate> planets) {
        ImmutableList<PlanetUpdate> migrations = ImmutableList.copyOf(planets.stream()
                .filter(pU -> pU.planetUpdateStatus != UPDATE)
                .collect(Collectors.toList()));
        ImmutableList<PlanetUpdate> updates = ImmutableList.copyOf(planets.stream()
                .filter(pU -> pU.planetUpdateStatus == UPDATE)
                .collect(Collectors.toList()));
        Set<PlanetNode> allOldPlanets = migrations.stream()
                .map(pU -> pU.oldPlanet)
                .collect(toSet());
        Set<PlanetNode> allDeletedPlanets = migrations.stream()
                .filter(pU -> pU.planetUpdateStatus == DELETE)
                .map(pU -> pU.oldPlanet)
                .collect(toSet());
        Set<PlanetNode> allNewPlanets = migrations.stream()
                .flatMap(pU -> pU.newPlanets.stream())
                .collect(toSet());

        ImmutableSet<PlanetNode> createdPlanets = ImmutableSet.copyOf(Sets.difference(allNewPlanets, allOldPlanets));
        ImmutableSet<PlanetNode> deletedPlanets = ImmutableSet.copyOf(allDeletedPlanets);
        List<Scope> scopes = Lists.newArrayList();
        graphDb.findNodes(Labels.SCOPE).forEachRemaining(node ->
                scopes.add(new Scope(Optional.ofNullable(node.getProperty(ID, null))
                        .orElseGet(() -> node.getProperty(NAME)).toString(),
                        node.getProperty(TAG).toString())));

        movePlanetTemplates(updates);
        updates.forEach(planetUpdate -> scopes.forEach(scope -> movePlanets(planetUpdate, scope)));

        createPlanetTemplates(createdPlanets);
        migrations.forEach(planetUpdate -> scopes.forEach(scope -> migratePlanets(planetUpdate, scope)));

        deletePlanetTemplates(deletedPlanets);
        if(!deletedPlanets.isEmpty()) scopes.forEach(scope -> deletePlanets(deletedPlanets, scope));
    }

    private void createPlanetTemplates(ImmutableSet<PlanetNode> createdPlanets) {
        createdPlanets.forEach(createdPlanet -> {
            UniqueEntity<Node> newPlanetTemplateNode = planetTemplateFactory.getOrCreateWithOutcome(NAME, createdPlanet.name);
            Set<MatchProperties> matchProperties = createdPlanet.attributes
                    .stream()
                    .map(a -> {
                        String[] split = a.split(":");
                        if (split.length != 2) {
                            throw new IllegalArgumentException("Malformed attribute " + a);
                        }
                        return MatchProperties.of(_TYPE, split[0], NAME, split[1]);
                    })
                    .collect(toSet());
            RelationshipUtils.replaceRelationships(OUTGOING, newPlanetTemplateNode.entity, ATTRIBUTE, attributeNodeFactory, matchProperties);
        });
    }

    private void migratePlanets(PlanetUpdate planetUpdate, Scope scope) {
        Optional<Node> oldNodeOpt = Optional.ofNullable(planetUpdate.oldPlanet).flatMap(v -> localizePlanet(v.name, scope));
        if (!oldNodeOpt.isPresent()) {
            return;
        }
        Node oldPlanetNode = oldNodeOpt.get();

        ImmutableSet<PlanetNode> newPlanets = planetUpdate.newPlanets;
        if (newPlanets.size() == 1) {
            PlanetNode newPlanet = Iterables.getOnlyElement(newPlanets);
            // Move all ne from old planets to new one
            Node newPlanetNode = planetFactory.getOrCreateWithOutcome(NAME, getPlanetName(newPlanet.name, scope)).entity;
            for (Relationship oldRelationship : oldPlanetNode.getRelationships(INCOMING, ATTRIBUTE)) {
                oldRelationship.getStartNode().createRelationshipTo(newPlanetNode, ATTRIBUTE);
                oldRelationship.delete();
            }
        } else {
            // Move all ne from old planet to best matching
            ImmutableMap<String, ImmutableSet<String>> newPlanetAttributes = ImmutableMap.copyOf(newPlanets.stream()
                    .collect(toMap(p -> p.name, p -> p.attributes)));
            PlanetByContext planetByContext = new PlanetByContext(newPlanetAttributes);
            Map<String, Node> newPlanetNodes = Maps.newHashMap();
            for (Relationship oldRelationship : oldPlanetNode.getRelationships(INCOMING, ATTRIBUTE)) {
                Node element = oldRelationship.getStartNode();
                String planetTemplateName = TemplatedPlanetFactory.localizePlanetForElement(element, planetByContext);
                Node newPlanetNode = newPlanetNodes.computeIfAbsent(planetTemplateName, k -> planetFactory.getOrCreateWithOutcome(NAME, getPlanetName(k, scope)).entity);
                oldRelationship.getStartNode().createRelationshipTo(newPlanetNode, ATTRIBUTE);
                oldRelationship.delete();
            }
        }
    }

    private void movePlanets(PlanetUpdate planetUpdate, Scope scope) {
        PlanetNode oldPlanet = planetUpdate.oldPlanet;
        PlanetNode newPlanet = Iterables.getOnlyElement(planetUpdate.newPlanets);
        localizePlanet(oldPlanet.name, scope).ifPresent(node ->
                node.setProperty(NAME, getPlanetName(newPlanet.name, scope)));

    }

    private void movePlanetTemplates(ImmutableList<PlanetUpdate> planetUpdates) {
        planetUpdates.forEach(planetUpdate -> {
            PlanetNode oldPlanet = planetUpdate.oldPlanet;
            Node oldPlanetNode = planetTemplateFactory.getWithOutcome(NAME, oldPlanet.name);
            PlanetNode newPlanet = Iterables.getOnlyElement(planetUpdate.newPlanets);
            if (!oldPlanet.name.equals(newPlanet.name)) {
                // Rename planet
                oldPlanetNode.setProperty(NAME, newPlanet.name);
            }
            if (!oldPlanet.attributes.equals(newPlanet.attributes)) {
                // Change planet attributes
                Set<MatchProperties> matchProperties = newPlanet.attributes
                        .stream()
                        .map(a -> {
                            String[] split = a.split(":");
                            if (split.length != 2) {
                                throw new IllegalArgumentException("Malformed attribute " + a);
                            }
                            return MatchProperties.of(_TYPE, split[0], NAME, split[1]);
                        })
                        .collect(toSet());
                RelationshipUtils.replaceRelationships(OUTGOING, oldPlanetNode, ATTRIBUTE, attributeNodeFactory, matchProperties);
            }
        });
    }

    private void deletePlanetTemplates(ImmutableSet<PlanetNode> deletedPlanets) {
        deletedPlanets.forEach(deletedPlanet ->
                Optional.ofNullable(planetTemplateFactory.getWithOutcome(NAME, deletedPlanet.name))
                        .ifPresent(node -> {
                            node.getRelationships().forEach(Relationship::delete);
                            node.delete();
                        }));

    }

    private void deletePlanets(ImmutableSet<PlanetNode> deletedPlanets, Scope scope) {
        deletedPlanets.stream()
                .map(p -> localizePlanet(p.name, scope))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(Node::delete);
    }


    public Optional<Node> localizePlanet(String planetTemplateName, Scope scope) {
        String planetName = planetTemplateName.replace(TemplatedPlanetFactory.PLACEHOLDER, scope.id);
        return Optional.ofNullable(planetFactory.getWithOutcome(NAME, planetName));
    }

    public String getPlanetName(String planetTemplateName, Scope scope) {
        return planetTemplateName.replace(TemplatedPlanetFactory.PLACEHOLDER, scope.id);
    }

    private ImmutableSet<Node> createRealmTemplates(Schema schema, ImmutableMap<String, CounterNode> counters) {
        Set<Node> realmTemplates = Sets.newHashSet();
        for (RealmNode realmNodeEntry : schema.realms.values()) {
            String realm = realmNodeEntry.name;
            UniqueEntity<Node> realmTemplateEntity = realmTemplateFactory.getOrCreateWithOutcome(NAME, realm);

            if (!realmTemplateEntity.wasCreated) {
                deleteTree(false, realmTemplateEntity.entity, OUTGOING, MEMDEXPATH);
            }

            createRealm(realmTemplateEntity, realmNodeEntry, counters);

            realmTemplates.add(realmTemplateEntity.entity);
        }
        return ImmutableSet.copyOf(realmTemplates);
    }

    private void mergeMemdexTree(Node parentNode, MemdexPathNode memdexPath, ImmutableMap<String, CounterNode> counters) {
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
        updateMemdexTree(memdexPath, counters, segmentNode);
    }

    private void updateMemdexTree(MemdexPathNode memdexPath, ImmutableMap<String, CounterNode> counters, Node segmentNode) {
//        replaceAttributesRelationships(memdexPath, segmentNode);

        updateCountersRelationships(memdexPath, counters, segmentNode);

        for (MemdexPathNode child : memdexPath.children) {
            mergeMemdexTree(segmentNode, child, counters);
        }
    }

    private void createRealm(UniqueEntity<Node> realmTemplateEntity, RealmNode realm, ImmutableMap<String, CounterNode> counters) {
        replaceAttributesRelationships(realm, realmTemplateEntity.entity);
        Node memdexPathNode = createMemdexTree(realm.memdexPath, counters);
        realmTemplateEntity.entity.createRelationshipTo(memdexPathNode, MEMDEXPATH);
    }

    private Node createMemdexTree(MemdexPathNode memdexPathNode, ImmutableMap<String, CounterNode> counters) {
        Node segmentNode = graphDb.createNode(Labels.SEGMENT);
        segmentNode.setProperty(PATH, memdexPathNode.segment);

        updateCountersRelationships(memdexPathNode, counters, segmentNode);

        String keyType = memdexPathNode.keyAttribute;
        String[] split = keyType.split(":");
        if (split.length < 2 || split.length > 3) {
            throw new IllegalArgumentException("Malformed attribute " + keyType);
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

        for (MemdexPathNode child : memdexPathNode.children) {
            Node childNode = createMemdexTree(child, counters);
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

    private void replaceAttributesRelationships(RealmNode realmNode, Node node) {
        node.getRelationships(OUTGOING, ATTRIBUTE).forEach(Relationship::delete);
        realmNode.attributes.forEach(a -> {
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
            Relationship relationshipTo = node.createRelationshipTo(attributeNode.entity, ATTRIBUTE);
            if (specializer == null) {
                relationshipTo.removeProperty(LINK_PROP_SPECIALIZER);
            } else {
                relationshipTo.setProperty(LINK_PROP_SPECIALIZER, specializer);
            }
        });
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
