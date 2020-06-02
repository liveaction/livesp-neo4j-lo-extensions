package com.livingobjects.neo4j.schema;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.helper.MatchProperties;
import com.livingobjects.neo4j.helper.PlanetByContext;
import com.livingobjects.neo4j.helper.PlanetFactory;
import com.livingobjects.neo4j.helper.RelationshipUtils;
import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.schema.CounterNode;
import com.livingobjects.neo4j.model.schema.MemdexPathNode;
import com.livingobjects.neo4j.model.schema.RealmNode;
import com.livingobjects.neo4j.model.schema.RealmPathSegment;
import com.livingobjects.neo4j.model.schema.SchemaAndPlanets;
import com.livingobjects.neo4j.model.schema.SchemaAndPlanetsUpdate;
import com.livingobjects.neo4j.model.schema.managed.CountersDefinition;
import com.livingobjects.neo4j.model.schema.managed.ManagedSchema;
import com.livingobjects.neo4j.model.schema.planet.PlanetNode;
import com.livingobjects.neo4j.model.schema.planet.PlanetUpdate;
import com.livingobjects.neo4j.model.schema.type.type.CounterType;
import com.livingobjects.neo4j.model.schema.update.SchemaUpdate;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.*;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.*;
import static com.livingobjects.neo4j.model.schema.planet.PlanetUpdateStatus.DELETE;
import static com.livingobjects.neo4j.model.schema.planet.PlanetUpdateStatus.UPDATE;
import static com.livingobjects.neo4j.model.schema.type.type.CounterType.COUNT;
import static java.util.stream.Collectors.*;
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

    private final PlanetFactory planetFactory;

    private final SchemaReader schemaReader;

    public SchemaLoader(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        this.schemaFactory = new UniqueElementFactory(graphDb, Labels.SCHEMA, Optional.empty());
        this.planetTemplateFactory = new UniqueElementFactory(graphDb, Labels.PLANET_TEMPLATE, Optional.empty());
        this.realmTemplateFactory = new UniqueElementFactory(graphDb, Labels.REALM_TEMPLATE, Optional.empty());
        this.attributeNodeFactory = new UniqueElementFactory(graphDb, Labels.ATTRIBUTE, Optional.empty());
        this.counterNodeFactory = new UniqueElementFactory(graphDb, Labels.COUNTER, Optional.of(Labels.KPI));
        this.planetFactory = new PlanetFactory(graphDb);
        this.schemaReader = new SchemaReader();
    }

    public boolean update(SchemaAndPlanetsUpdate schemaAndPlanetsUpdate) {
        return lockAndWriteSchema(tx -> {

            boolean modified = migrateSchemas(schemaAndPlanetsUpdate.schemaUpdates, schemaAndPlanetsUpdate.version, tx);

            migratePlanets(schemaAndPlanetsUpdate.planetUpdates, tx);

            return modified;

        });
    }

    public boolean load(SchemaAndPlanets schemaAndPlanets) {
        return lockAndWriteSchema(tx -> {
            ManagedSchema managedSchema = ManagedSchema.fullyManaged(schemaAndPlanets.schema);
            ImmutableList<PlanetUpdate> planetUpdate = schemaAndPlanets.planetMigrations;
            UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schemaAndPlanets.schema.id);
            schemaNode.entity.setProperty(VERSION, schemaAndPlanets.schema.version);
            if (!schemaNode.wasCreated) {
                managedSchema = mergeAllUnmanagedCounters(managedSchema, schemaNode.entity);
                for (Relationship realmRelationship : schemaNode.entity.getRelationships(OUTGOING, PROVIDED)) {
                    Node realmTemplateNode = realmRelationship.getEndNode();
                    realmRelationship.delete();
                    deleteRealm(true, realmTemplateNode, OUTGOING, MEMDEXPATH);
                }
            }

            ImmutableSet<Node> realmTemplates = createRealmTemplates(managedSchema, tx);

            RelationshipUtils.replaceRelationships(OUTGOING, schemaNode.entity, RelationshipTypes.PROVIDED, realmTemplates);

            migratePlanets(planetUpdate, tx);

            return true;
        });
    }

    private ManagedSchema mergeAllUnmanagedCounters(ManagedSchema managedSchema, Node node) {
        Map<String, RealmNode> mergedRealms = managedSchema.realms.values().stream()
                .collect(toMap(r -> r.name, r -> r));
        CountersDefinition.Builder countersDefinitionBuilder = CountersDefinition.builder().addFullyManaged(managedSchema.counters.counters);

        for (Relationship relationship : node.getRelationships(OUTGOING, PROVIDED)) {
            Node realmTemplateNode = relationship.getEndNode();
            if (realmTemplateNode.hasLabel(Labels.REALM_TEMPLATE)) {
                String name = realmTemplateNode.getProperty(NAME).toString();
                RealmNode inputRealm = mergedRealms.get(name);
                if (inputRealm != null) {
                    mergedRealms.put(name, mergeManagedWithUnmanagedRealm(inputRealm, realmTemplateNode, countersDefinitionBuilder));
                } else {
                    schemaReader.readRealm(realmTemplateNode, true, countersDefinitionBuilder)
                            .ifPresent(r -> mergedRealms.put(r.name, r));
                }
            }
        }


        return new ManagedSchema(ImmutableMap.copyOf(mergedRealms), countersDefinitionBuilder.build());
    }

    private RealmNode mergeManagedWithUnmanagedRealm(RealmNode managedRealm, Node unmanagedRealm, CountersDefinition.Builder countersDefinitionBuilder) {
        Iterator<Relationship> subPathsIterator = unmanagedRealm.getRelationships(OUTGOING, MEMDEXPATH).iterator();
        boolean hasSubPaths = subPathsIterator.hasNext();
        if (!hasSubPaths) {
            return managedRealm;
        }
        Relationship mdxPathRelationship = subPathsIterator.next();
        Node segmentNode = mdxPathRelationship.getEndNode();
        if (segmentNode.hasLabel(Labels.SEGMENT)) {
            Object existingPath = segmentNode.getProperty(PATH);

            if (existingPath.equals(managedRealm.memdexPath.segment)) {
                return new RealmNode(managedRealm.name, managedRealm.attributes, mergeManagedWithUnmanagedMemdexPath(managedRealm.memdexPath, segmentNode, countersDefinitionBuilder));
            } else {
                Optional<MemdexPathNode> previousMemdexPath = schemaReader.readMemdexPath(segmentNode, true, CountersDefinition.builder());
                if (previousMemdexPath.isPresent()) {
                    throw new IllegalArgumentException(String.format("Realm %s can have only one root level : %s != %s", managedRealm.name, existingPath, managedRealm.memdexPath.segment));
                } else {
                    return managedRealm;
                }
            }
        } else {
            return managedRealm;
        }
    }

    private MemdexPathNode mergeManagedWithUnmanagedMemdexPath(MemdexPathNode managedMemdexPath, Node unmanagedRealm, CountersDefinition.Builder countersDefinitionBuilder) {
        List<String> mergedCounters = Lists.newArrayList(managedMemdexPath.counters);
        List<MemdexPathNode> mergedChildren = Lists.newArrayList();

        Map<String, MemdexPathNode> children = managedMemdexPath.children.stream()
                .collect(toMap(m -> m.segment, m -> m));

        List<String> counters = SchemaReader.readAllCounters(unmanagedRealm, true, countersDefinitionBuilder);
        mergedCounters.addAll(counters);

        Set<String> notFound = Sets.newLinkedHashSet(children.keySet());
        for (Relationship relationship : unmanagedRealm.getRelationships(OUTGOING, MEMDEXPATH)) {
            Node segmentNode = relationship.getEndNode();
            if (segmentNode.hasLabel(Labels.SEGMENT)) {
                String path = segmentNode.getProperty(PATH).toString();
                if (notFound.remove(path)) {
                    MemdexPathNode managedChild = children.get(path);
                    if (managedChild != null) {
                        mergedChildren.add(mergeManagedWithUnmanagedMemdexPath(managedChild, segmentNode, countersDefinitionBuilder));
                    } else {
                        schemaReader.readMemdexPath(segmentNode, true, countersDefinitionBuilder)
                                .ifPresent(mergedChildren::add);
                    }
                } else {
                    schemaReader.readMemdexPath(segmentNode, true, countersDefinitionBuilder)
                            .ifPresent(mergedChildren::add);
                }
            }
        }

        for (String path : notFound) {
            mergedChildren.add(children.get(path));
        }

        return new MemdexPathNode(managedMemdexPath.segment, managedMemdexPath.keyAttribute, mergedCounters, mergedChildren, managedMemdexPath.topCount);
    }

    private boolean migrateSchemas(ImmutableList<SchemaUpdate> schemaUpdates, String version, Transaction tx) {
        boolean modified = false;
        for (SchemaUpdate schemaUpdate : schemaUpdates) {
            if (applySchemaUpdate(schemaUpdate, version, tx)) {
                modified = true;
            }
        }
        return modified;
    }

    private boolean applySchemaUpdate(SchemaUpdate schemaUpdate, String version, Transaction tx) {
        return schemaUpdate.visit(new SchemaUpdate.SchemaUpdateVisitor<>() {
            @Override
            public Boolean appendCounter(String schema, String realmTemplate, ImmutableSet<String> attributes, ImmutableList<RealmPathSegment> realmPath, CounterNode counter) {
                return appendCounterToSchema(schema, realmTemplate, attributes, realmPath, counter, version, tx);
            }

            @Override
            public Boolean deleteCounter(String schema, String realmTemplate, ImmutableList<RealmPathSegment> realmPath, String counter) {
                return deleteCounterFromSchema(schema, realmTemplate, realmPath, counter);
            }
        });
    }

    private boolean deleteCounterFromSchema(String schema,
                                            String realmTemplate,
                                            ImmutableList<RealmPathSegment> realmPath,
                                            String counter) {
        boolean modified = false;
        Node schemaNode = schemaFactory.getWithOutcome(ID, schema);
        if (schemaNode != null) {
            for (Relationship realmTemplateRelationship : schemaNode.getRelationships(OUTGOING, PROVIDED)) {
                Node realmNode = realmTemplateRelationship.getEndNode();
                if (realmNode.hasLabel(Labels.REALM_TEMPLATE)) {
                    String realm = realmNode.getProperty(NAME).toString();

                    if (realmTemplate.equals(realm)) {

                        if (!realmPath.isEmpty()) {
                            if (deleteCounterFromRealmPath(realmPath, counter, realmNode)) {
                                modified = true;
                            }

                            boolean hasSubPaths = realmNode.getRelationships(OUTGOING, MEMDEXPATH).iterator().hasNext();
                            if (!hasSubPaths) {
                                realmNode.getRelationships().forEach(Relationship::delete);
                                realmNode.delete();
                            }
                        }

                        break;

                    }
                }
            }
        }
        return modified;
    }

    private boolean deleteCounterFromRealmPath(ImmutableList<RealmPathSegment> realmPath,
                                               String counter,
                                               Node parentNode) {
        boolean deleted = false;

        RealmPathSegment rootSegment = realmPath.get(0);
        ImmutableList<RealmPathSegment> tail = realmPath.subList(1, realmPath.size());

        for (Relationship relationship : parentNode.getRelationships(OUTGOING, MEMDEXPATH)) {
            Node memdexPathNode = relationship.getEndNode();

            if (memdexPathNode.hasLabel(Labels.SEGMENT)) {
                String path = memdexPathNode.getProperty(PATH).toString();
                if (path.equals(rootSegment.path)) {
                    if (tail.isEmpty()) {
                        for (Relationship counterRelationship : memdexPathNode.getRelationships(INCOMING, PROVIDED)) {
                            Node counterNode = counterRelationship.getStartNode();
                            if (counterNode.hasLabel(Labels.COUNTER)) {
                                String counterName = counterNode.getProperty(NAME).toString();
                                if (counterName.equals(counter)) {

                                    if (deleteCounter(counterRelationship, counterNode)) {
                                        deleted = true;
                                    }
                                    boolean hasCounters = memdexPathNode.getRelationships(INCOMING, PROVIDED).iterator().hasNext();
                                    boolean hasSubPaths = memdexPathNode.getRelationships(OUTGOING, MEMDEXPATH).iterator().hasNext();
                                    if (!hasCounters && !hasSubPaths) {
                                        memdexPathNode.getRelationships().forEach(Relationship::delete);
                                        memdexPathNode.delete();
                                        deleted = true;
                                    }
                                    break;
                                }
                            }
                        }

                    } else {
                        if (deleteCounterFromRealmPath(tail, counter, memdexPathNode)) {
                            deleted = true;
                        }
                    }
                    if (deleted) {
                        boolean hasCounters = parentNode.getRelationships(INCOMING, PROVIDED).iterator().hasNext();
                        boolean hasSubPaths = parentNode.getRelationships(OUTGOING, MEMDEXPATH).iterator().hasNext();
                        if (!hasCounters && !hasSubPaths) {
                            parentNode.getRelationships().forEach(Relationship::delete);
                            parentNode.delete();
                        }
                    }
                }
            }
        }

        return deleted;
    }

    private boolean deleteCounter(Relationship providedRelationship, Node counterNode) {
        if (!SchemaReader.isManaged(counterNode)) {

            providedRelationship.delete();
            if (!counterNode.hasRelationship(INCOMING, VAR)) {
                if (!counterNode.hasRelationship(OUTGOING, PROVIDED)) {
                    counterNode.delete();
                }
            }

            return true;
        }
        return false;
    }

    private boolean appendCounterToSchema(String schemaId,
                                          String realmTemplate,
                                          ImmutableSet<String> attributes,
                                          ImmutableList<RealmPathSegment> realmPath,
                                          CounterNode counter,
                                          String version,
                                          Transaction tx) {
        boolean modified = false;

        UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schemaId);
        String oldVersion = schemaNode.entity.getProperty(VERSION, "").toString();
        if (version != null && !oldVersion.equals(version)) {
            schemaNode.entity.setProperty(VERSION, version);
            modified = true;
        }
        if (version == null && oldVersion.isEmpty()) {
            schemaNode.entity.setProperty(VERSION, "0.1");
            modified = true;
        }

        UniqueEntity<Node> realmTemplateEntity = realmTemplateFactory.getOrCreateWithOutcome(NAME, realmTemplate);
        if (realmTemplateEntity.wasCreated) {
            modified = true;
        }

        Iterable<Relationship> relationships = realmTemplateEntity.entity.getRelationships(INCOMING, PROVIDED);
        boolean moveRealm = false;
        for (Relationship relationship : relationships) {
            Node schema = relationship.getStartNode();
            String existingSchemaId = schema.getProperty(ID, "").toString();
            if (!existingSchemaId.equalsIgnoreCase(schemaId)) {
                relationship.delete();
                moveRealm = true;
            }
        }

        if (updateRealmAttributes(attributes, realmTemplateEntity)) {
            modified = true;
        }

        if (!realmPath.isEmpty()) {
            RealmPathSegment rootSegment = realmPath.get(0);
            ImmutableList<RealmPathSegment> tailPath = realmPath.subList(1, realmPath.size());

            Relationship firstLevel = realmTemplateEntity.entity.getSingleRelationship(MEMDEXPATH, OUTGOING);
            if (firstLevel == null) {
                if (mergeRealmPath(realmTemplate, realmTemplateEntity.entity, rootSegment, tailPath, counter, tx)) {
                    modified = true;
                }
            } else {
                Node firstSegment = firstLevel.getEndNode();

                Object existingPath = firstSegment.getProperty(PATH);

                if (existingPath.equals(rootSegment.path)) {
                    if (updateRealmPath(realmTemplate, rootSegment, tailPath, counter, firstSegment, tx)) {
                        modified = true;
                    }
                } else {
                    throw new IllegalArgumentException(String.format("Realm %s can have only one root level : %s != %s", realmTemplate, existingPath, rootSegment.path));
                }
            }
        }

        if (realmTemplateEntity.wasCreated || moveRealm) {
            for (Relationship relationship : realmTemplateEntity.entity.getRelationships(INCOMING, PROVIDED)) {
                relationship.delete();
            }
            schemaNode.entity.createRelationshipTo(realmTemplateEntity.entity, PROVIDED);
        }
        return modified;
    }

    private boolean updateRealmAttributes(Collection<String> attributes, UniqueEntity<Node> realmTemplateEntity) {
        Set<MatchProperties> attributesMatchProperties = attributes.stream()
                .map(attribute -> {
                    String[] split = attribute.split(":");
                    if (split.length < 2 || split.length > 3) {
                        throw new IllegalArgumentException("Malformed attribute " + attribute);
                    }
                    String type = split[0];
                    String name = split[1];
                    return MatchProperties.of(_TYPE, type, NAME, name);
                })
                .collect(toSet());
        return RelationshipUtils.replaceRelationships(OUTGOING, realmTemplateEntity.entity, ATTRIBUTE, attributeNodeFactory, attributesMatchProperties);
    }

    private void migratePlanets(ImmutableList<PlanetUpdate> planets, Transaction tx) {
        ImmutableList<PlanetUpdate> migrations = ImmutableList.copyOf(planets.stream()
                .filter(pU -> pU.planetUpdateStatus != UPDATE)
                .collect(toList()));
        ImmutableList<PlanetUpdate> updates = ImmutableList.copyOf(planets.stream()
                .filter(pU -> pU.planetUpdateStatus == UPDATE)
                .collect(toList()));
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
        tx.findNodes(Labels.SCOPE).forEachRemaining(node ->
                scopes.add(new Scope(Optional.ofNullable(node.getProperty(ID, null))
                        .orElseGet(() -> node.getProperty(NAME)).toString(),
                        node.getProperty(TAG).toString())));

        movePlanetTemplates(updates);
        updates.forEach(planetUpdate -> scopes.forEach(scope -> movePlanets(planetUpdate, scope)));

        createPlanetTemplates(createdPlanets);
        migrations.forEach(planetUpdate -> scopes.forEach(scope -> migratePlanets(planetUpdate, scope)));

        deletePlanetTemplates(deletedPlanets);
        if (!deletedPlanets.isEmpty()) scopes.forEach(scope -> deletePlanets(deletedPlanets, scope));
    }

    private void createPlanetTemplates(ImmutableSet<PlanetNode> createdPlanets) {
        createdPlanets.forEach(createdPlanet -> {
            UniqueEntity<Node> newPlanetTemplateNode = planetTemplateFactory.getOrCreateWithOutcome(NAME, createdPlanet.name);
            Set<MatchProperties> matchProperties = matchProperties(createdPlanet);
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
        if (newPlanets.size() == 1 && planetUpdate.planetUpdateStatus == DELETE) {
            PlanetNode newPlanet = Iterables.getOnlyElement(newPlanets);
            // Move all ne from old planets to new one
            Node newPlanetNode = planetFactory.getOrCreate(newPlanet.name, scope).entity;
            for (Relationship oldRelationship : oldPlanetNode.getRelationships(INCOMING, ATTRIBUTE)) {
                oldRelationship.getStartNode().createRelationshipTo(newPlanetNode, ATTRIBUTE);
                oldRelationship.delete();
            }
        } else {
            // Move all ne from old planet to best matching
            ImmutableSet.Builder<PlanetNode> targetPlanetsBuilder = ImmutableSet.builder();
            if (planetUpdate.planetUpdateStatus != DELETE) {
                targetPlanetsBuilder.add(planetUpdate.oldPlanet);
            }
            targetPlanetsBuilder.addAll(newPlanets);
            ImmutableMap<String, ImmutableSet<String>> newPlanetAttributes = ImmutableMap.copyOf(targetPlanetsBuilder.build()
                    .stream()
                    .collect(toMap(p -> p.name, p -> p.attributes)));
            PlanetByContext planetByContext = new PlanetByContext(newPlanetAttributes);
            Map<String, Node> newPlanetNodes = Maps.newHashMap();
            for (Relationship oldRelationship : oldPlanetNode.getRelationships(INCOMING, ATTRIBUTE)) {
                Node element = oldRelationship.getStartNode();
                String planetTemplateName = TemplatedPlanetFactory.localizePlanetForElement(element, planetByContext);
                Node newPlanetNode = newPlanetNodes.computeIfAbsent(planetTemplateName, k -> planetFactory.getOrCreate(k, scope).entity);
                oldRelationship.getStartNode().createRelationshipTo(newPlanetNode, ATTRIBUTE);
                oldRelationship.delete();
            }
        }
    }

    private void movePlanets(PlanetUpdate planetUpdate, Scope scope) {
        PlanetNode oldPlanet = planetUpdate.oldPlanet;
        PlanetNode newPlanet = Iterables.getOnlyElement(planetUpdate.newPlanets);
        localizePlanet(oldPlanet.name, scope).ifPresent(node ->
                node.setProperty(NAME, planetFactory.getPlanetName(newPlanet.name, scope)));

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
                Set<MatchProperties> matchProperties = matchProperties(newPlanet);
                RelationshipUtils.replaceRelationships(OUTGOING, oldPlanetNode, ATTRIBUTE, attributeNodeFactory, matchProperties);
            }
        });
    }

    private Set<MatchProperties> matchProperties(PlanetNode newPlanet) {
        return newPlanet.attributes
                .stream()
                .map(a -> {
                    String[] split = a.split(":");
                    if (split.length != 2) {
                        throw new IllegalArgumentException("Malformed attribute " + a);
                    }
                    return MatchProperties.of(_TYPE, split[0], NAME, split[1]);
                })
                .collect(toSet());
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

    private Optional<Node> localizePlanet(String planetTemplateName, Scope scope) {
        return Optional.ofNullable(planetFactory.get(planetTemplateName, scope));
    }

    private ImmutableSet<Node> createRealmTemplates(ManagedSchema managedSchema, Transaction tx) {
        Set<Node> realmTemplates = Sets.newLinkedHashSet();
        for (RealmNode realmNodeEntry : managedSchema.realms.values()) {
            String realm = realmNodeEntry.name;
            UniqueEntity<Node> realmTemplateEntity = realmTemplateFactory.getOrCreateWithOutcome(NAME, realm);

            if (!realmTemplateEntity.wasCreated) {
                deleteRealm(false, realmTemplateEntity.entity, OUTGOING, MEMDEXPATH);
                realmTemplateEntity.entity.getRelationships(INCOMING, PROVIDED).forEach(Relationship::delete);
            }

            createRealm(realmTemplateEntity, realmNodeEntry, managedSchema, tx);

            realmTemplates.add(realmTemplateEntity.entity);
        }
        return ImmutableSet.copyOf(realmTemplates);
    }

    private boolean mergeRealmPath(String realmTemplate,
                                   Node parentNode,
                                   RealmPathSegment segment,
                                   ImmutableList<RealmPathSegment> tail,
                                   CounterNode counter,
                                   Transaction tx) {
        boolean modified = false;

        Node segmentNode = null;
        for (Relationship relationship : parentNode.getRelationships(OUTGOING, MEMDEXPATH)) {
            Node childNode = relationship.getEndNode();
            String path = childNode.getProperty(PATH, "").toString();
            if (segment.path.equals(path)) {
                segmentNode = childNode;
                break;
            }
        }
        if (segmentNode == null) {
                segmentNode = tx.createNode(Labels.SEGMENT);
                segmentNode.setProperty(PATH, segment.path);
                parentNode.createRelationshipTo(segmentNode, MEMDEXPATH);
            modified = true;
        }

        if (updateRealmPath(realmTemplate, segment, tail, counter, segmentNode, tx)) {
            modified = true;
        }

        return modified;
    }

    private boolean updateRealmPath(String realmTemplate,
                                    RealmPathSegment segment,
                                    ImmutableList<RealmPathSegment> tail,
                                    CounterNode counter,
                                    Node segmentNode,
                                    Transaction tx) {
        boolean modified = false;

        if (replaceAttributesRelationships(ImmutableSet.of(segment.keyAttribute), segmentNode)) {
            modified = true;
        }

        if (tail.isEmpty()) {
            if (appendCounterToSegment(realmTemplate, counter, segmentNode)) {
                modified = true;
            }
        } else {
            if (mergeRealmPath(realmTemplate, segmentNode, tail.get(0), tail.subList(1, tail.size()), counter, tx)) {
                modified = true;
            }
        }
        return modified;
    }

    private void createRealm(UniqueEntity<Node> realmTemplateEntity, RealmNode realm, ManagedSchema managedSchema, Transaction tx) {
        replaceAttributesRelationships(realm.attributes, realmTemplateEntity.entity);
        Node memdexPathNode = createMemdexTree(realm.name, realm.memdexPath, managedSchema, tx);
        realmTemplateEntity.entity.createRelationshipTo(memdexPathNode, MEMDEXPATH);
    }

    private Node createMemdexTree(String realmTemplate, MemdexPathNode memdexPathNode, ManagedSchema managedSchema, Transaction tx) {
        Node segmentNode = tx.createNode(Labels.SEGMENT);
        segmentNode.setProperty(PATH, memdexPathNode.segment);
        if (memdexPathNode.topCount != null) {
            segmentNode.setProperty("topCount", memdexPathNode.topCount);
        }

        updateCountersRelationships(realmTemplate, memdexPathNode, managedSchema, segmentNode);

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
            Node childNode = createMemdexTree(realmTemplate, child, managedSchema, tx);
            segmentNode.createRelationshipTo(childNode, MEMDEXPATH);
        }

        return segmentNode;
    }

    private boolean appendCounterToSegment(String realmTemplate, CounterNode counter, Node segmentNode) {
        Relationship relationship = null;
        for (Relationship existingRelationship : segmentNode.getRelationships(INCOMING, PROVIDED)) {
            Node counterNode = existingRelationship.getStartNode();
            if (counter.name.equals(counterNode.getProperty(NAME).toString())) {
                relationship = existingRelationship;
                break;
            }
        }

        String counterId = counter.name + "@context:" + realmTemplate;
        UniqueEntity<Node> counterEntity = counterNodeFactory.getOrCreateWithOutcome(ID, counterId);
        counterEntity.entity.setProperty(NAME, counter.name);
        counterEntity.entity.setProperty(CONTEXT, realmTemplate);
        counterEntity.entity.setProperty(_TYPE, "counter");

        if (counter.description == null) {
            counterEntity.entity.removeProperty(DESCRIPTION);
        } else {
            counterEntity.entity.setProperty(_TYPE, "counter");
        }
        counterEntity.entity.setProperty("defaultAggregation", counter.defaultAggregation);
        if (counter.defaultValue == null) {
            counterEntity.entity.removeProperty("defaultValue");
        } else {
            counterEntity.entity.setProperty("defaultValue", counter.defaultValue);
        }

        counterEntity.entity.setProperty("valueType", counter.valueType);
        counterEntity.entity.setProperty("unit", counter.unit);

        if (relationship == null) {
            counterEntity.entity.createRelationshipTo(segmentNode, PROVIDED);
        }

        return true;
    }

    private void updateCountersRelationships(String realmTemplate,
                                             MemdexPathNode memdexPath,
                                             ManagedSchema managedSchema,
                                             Node segmentNode) {
        Map<String, Relationship> existingRelationships = Maps.newHashMap();
        for (Relationship relationship : segmentNode.getRelationships(INCOMING, PROVIDED)) {
            Node counterNode = relationship.getStartNode();
            String name = counterNode.getProperty(NAME).toString();
            existingRelationships.put(name, relationship);
        }

        for (String counter : memdexPath.counters) {
            CounterNode counterNode = managedSchema.counters.get(counter);
            if (counterNode == null) {
                throw new IllegalArgumentException(String.format("Counter with reference '%s' is not found in provided schema.", counter));
            }
            String counterId = counterNode.name + "@context:" + realmTemplate;
            UniqueEntity<Node> counterNodeEntity = counterNodeFactory.getOrCreateWithOutcome(ID, counterId);
            counterNodeEntity.entity.setProperty(NAME, counterNode.name);
            counterNodeEntity.entity.setProperty(CONTEXT, realmTemplate);
            counterNodeEntity.entity.setProperty("_type", "counter");
            counterNodeEntity.entity.setProperty(MANAGED, managedSchema.counters.isManaged(counter));
            counterNodeEntity.entity.setProperty("defaultAggregation", counterNode.defaultAggregation);
            if (counterNode.defaultValue == null) {
                counterNodeEntity.entity.removeProperty("defaultValue");
            } else {
                counterNodeEntity.entity.setProperty("defaultValue", counterNode.defaultValue);
            }
            counterNodeEntity.entity.setProperty("valueType", counterNode.valueType);
            counterNodeEntity.entity.setProperty("unit", counterNode.unit);
            if (counterNode.counterType == null) {
                counterNodeEntity.entity.removeProperty("type");
                counterNodeEntity.entity.removeProperty("countType");
            } else {
                counterNode.counterType.visit((CounterType.Visitor<Void>) countCounterType -> {
                    counterNodeEntity.entity.setProperty("type", COUNT);
                    counterNodeEntity.entity.setProperty("countType", countCounterType.countType);
                    return null;
                });
            }

            Relationship relationshipTo = existingRelationships.remove(counterNode.name);
            if (relationshipTo == null) {
                counterNodeEntity.entity.createRelationshipTo(segmentNode, PROVIDED);
            }
        }

        for (Map.Entry<String, Relationship> counterRelationship : existingRelationships.entrySet()) {
            Relationship relationship = counterRelationship.getValue();
            Node counterNode = relationship.getStartNode();
            deleteCounter(relationship, counterNode);
        }
    }

    private boolean replaceAttributesRelationships(ImmutableSet<String> attributes, Node node) {
        boolean modified = false;

        Map<String, Relationship> existingRelationships = Maps.newHashMap();
        for (Relationship relationship : node.getRelationships(OUTGOING, ATTRIBUTE)) {
            Node attributeNode = relationship.getEndNode();
            String a = attributeNode.getProperty(_TYPE).toString() + ':' + attributeNode.getProperty(NAME).toString();
            if (attributes.contains(a)) {
                existingRelationships.put(a, relationship);
            } else {
                relationship.delete();
                modified = true;
            }
        }

        for (String attribute : attributes) {
            Relationship relationship = existingRelationships.get(attribute);

            String[] split = attribute.split(":");
            if (split.length < 2 || split.length > 3) {
                throw new IllegalArgumentException("Malformed attribute " + attribute);
            }
            String type = split[0];
            String name = split[1];
            String specializer = null;
            if (split.length == 3) {
                specializer = split[2];
            }

            if (relationship == null) {
                UniqueEntity<Node> attributeNode = attributeNodeFactory.getOrCreateWithOutcome(_TYPE, type, NAME, name);
                Relationship relationshipTo = node.createRelationshipTo(attributeNode.entity, ATTRIBUTE);
                if (specializer == null) {
                    relationshipTo.removeProperty(LINK_PROP_SPECIALIZER);
                } else {
                    relationshipTo.setProperty(LINK_PROP_SPECIALIZER, specializer);
                }
                modified = true;
            } else {
                if (specializer == null) {
                    if (relationship.getProperty(LINK_PROP_SPECIALIZER, null) != null) {
                        relationship.removeProperty(LINK_PROP_SPECIALIZER);
                        modified = true;
                    }
                } else {
                    String existingSpecializer = relationship.getProperty(LINK_PROP_SPECIALIZER, "").toString();
                    if (!existingSpecializer.equals(specializer)) {
                        relationship.setProperty(LINK_PROP_SPECIALIZER, specializer);
                        modified = true;
                    }
                }
            }
        }
        return modified;
    }

    private void deleteRealm(boolean deleteRoot, Node root, Direction direction, org.neo4j.graphdb.RelationshipType relationshipType) {
        Iterable<Relationship> relationships = root.getRelationships(direction, relationshipType);
        for (Relationship relationship : relationships) {
            Node otherNode = relationship.getOtherNode(root);
            deleteRealm(true, otherNode, direction, relationshipType);
        }
        if (deleteRoot) {
            for (Relationship relationship : root.getRelationships(INCOMING, PROVIDED)) {
                Node counterNode = relationship.getStartNode();
                if (counterNode.hasLabel(Labels.COUNTER)) {
                    deleteCounter(relationship, counterNode);
                }
            }
            root.getRelationships().forEach(Relationship::delete);
            root.delete();
        }
    }

    private <T> T lockAndWriteSchema(Function<Transaction, T> schemaModification) {
        try {
            schemaLock.lock();
            try (Transaction tx = graphDb.beginTx()) {
                T result = schemaModification.apply(tx);
                tx.commit();
                return result;
            }
        } catch (Throwable e) {
            throw new RuntimeException(e);
        } finally {
            schemaLock.unlock();
        }
    }

}
