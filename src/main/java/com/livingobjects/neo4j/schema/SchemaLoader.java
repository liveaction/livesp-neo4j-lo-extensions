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
import com.livingobjects.neo4j.helper.PlanetFactory;
import com.livingobjects.neo4j.helper.RelationshipUtils;
import com.livingobjects.neo4j.helper.TemplatedPlanetFactory;
import com.livingobjects.neo4j.helper.UniqueElementFactory;
import com.livingobjects.neo4j.helper.UniqueEntity;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import com.livingobjects.neo4j.model.schema.CounterNode;
import com.livingobjects.neo4j.model.schema.MemdexPathNode;
import com.livingobjects.neo4j.model.schema.RealmNode;
import com.livingobjects.neo4j.model.schema.RealmPathSegment;
import com.livingobjects.neo4j.model.schema.Schema;
import com.livingobjects.neo4j.model.schema.SchemaAndPlanets;
import com.livingobjects.neo4j.model.schema.SchemaAndPlanetsUpdate;
import com.livingobjects.neo4j.model.schema.planet.PlanetNode;
import com.livingobjects.neo4j.model.schema.planet.PlanetUpdate;
import com.livingobjects.neo4j.model.schema.update.SchemaUpdate;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.LINK_PROP_SPECIALIZER;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.PATH;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.VERSION;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.ATTRIBUTE;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.MEMDEXPATH;
import static com.livingobjects.neo4j.model.iwan.RelationshipTypes.PROVIDED;
import static com.livingobjects.neo4j.model.schema.planet.PlanetUpdateStatus.DELETE;
import static com.livingobjects.neo4j.model.schema.planet.PlanetUpdateStatus.UPDATE;
import static java.util.stream.Collectors.toMap;
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

    private final PlanetFactory planetFactory;

    public SchemaLoader(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        this.schemaFactory = new UniqueElementFactory(graphDb, Labels.SCHEMA, Optional.empty());
        this.planetTemplateFactory = new UniqueElementFactory(graphDb, Labels.PLANET_TEMPLATE, Optional.empty());
        this.realmTemplateFactory = new UniqueElementFactory(graphDb, Labels.REALM_TEMPLATE, Optional.empty());
        this.attributeNodeFactory = new UniqueElementFactory(graphDb, Labels.ATTRIBUTE, Optional.empty());
        this.counterNodeFactory = new UniqueElementFactory(graphDb, Labels.COUNTER, Optional.of(Labels.KPI));
        this.planetFactory = new PlanetFactory(graphDb);
    }

    public boolean update(SchemaAndPlanetsUpdate schemaAndPlanetsUpdate) {
        return lockAndWriteSchema(() -> {

            boolean modified = migrateSchemas(schemaAndPlanetsUpdate.schemaUpdates);

            migratePlanets(schemaAndPlanetsUpdate.planetUpdates);

            return modified;

        });
    }

    public boolean load(SchemaAndPlanets schemaAndPlanets) {
        return lockAndWriteSchema(() -> {
            Schema schema = schemaAndPlanets.schema;
            ImmutableList<PlanetUpdate> planetUpdate = schemaAndPlanets.planetMigrations;
            UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schema.id);
            schemaNode.entity.setProperty(VERSION, schema.version);

            ImmutableSet<Node> realmTemplates = createRealmTemplates(schema, schema.counters);

            RelationshipUtils.replaceRelationships(OUTGOING, schemaNode.entity, RelationshipTypes.PROVIDED, realmTemplates,
                    node -> deleteTree(true, node, OUTGOING, MEMDEXPATH));

            migratePlanets(planetUpdate);

            return schemaNode.wasCreated;
        });
    }

    private boolean migrateSchemas(ImmutableList<SchemaUpdate> schemaUpdates) {
        boolean modified = false;
        for (SchemaUpdate schemaUpdate : schemaUpdates) {
            if (applySchemaUpdate(schemaUpdate)) {
                modified = true;
            }
        }
        return modified;
    }

    private boolean applySchemaUpdate(SchemaUpdate schemaUpdate) {
        return schemaUpdate.visit(new SchemaUpdate.SchemaUpdateVisitor<Boolean>() {
            @Override
            public Boolean appendCounter(String schema, String realmTemplate, ImmutableSet<String> attributes, ImmutableList<RealmPathSegment> realmPath, CounterNode counter) {
                return appendCounterToSchema(schema, realmTemplate, attributes, realmPath, counter);
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
        Node schemaNode = schemaFactory.getWithOutcome(NAME, schema);
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

                                    counterRelationship.delete();
                                    if (!counterNode.hasRelationship()) {
                                        counterNode.delete();
                                    }
                                    boolean hasCounters = parentNode.getRelationships(INCOMING, PROVIDED).iterator().hasNext();
                                    boolean hasSubPaths = parentNode.getRelationships(OUTGOING, MEMDEXPATH).iterator().hasNext();
                                    if (!hasCounters && !hasSubPaths) {
                                        parentNode.getRelationships().forEach(Relationship::delete);
                                        parentNode.delete();
                                    }

                                    deleted = true;
                                    break;
                                }
                            }
                        }

                    } else {
                        if (deleteCounterFromRealmPath(tail, counter, memdexPathNode)) {

                            boolean hasCounters = parentNode.getRelationships(INCOMING, PROVIDED).iterator().hasNext();
                            boolean hasSubPaths = parentNode.getRelationships(OUTGOING, MEMDEXPATH).iterator().hasNext();
                            if (!hasCounters && !hasSubPaths) {
                                parentNode.getRelationships().forEach(Relationship::delete);
                                parentNode.delete();
                            }

                            deleted = true;
                        }
                    }
                }
            }
        }

        return deleted;
    }

    private boolean appendCounterToSchema(String schemaId,
                                          String realmTemplate,
                                          ImmutableSet<String> attributes,
                                          ImmutableList<RealmPathSegment> realmPath,
                                          CounterNode counter) {
        boolean modified = false;

        UniqueEntity<Node> schemaNode = schemaFactory.getOrCreateWithOutcome(ID, schemaId);
        if (schemaNode.wasCreated) {
            schemaNode.entity.setProperty(VERSION, "1.0");
            modified = true;
        }

        UniqueEntity<Node> realmTemplateEntity = realmTemplateFactory.getOrCreateWithOutcome(NAME, realmTemplate);
        if (realmTemplateEntity.wasCreated) {
            modified = true;
        }

        Iterable<Relationship> relationships = realmTemplateEntity.entity.getRelationships(INCOMING, PROVIDED);
        for (Relationship relationship : relationships) {
            Node schema = relationship.getStartNode();
            String existingSchemaId = schema.getProperty(ID, "").toString();
            if (!existingSchemaId.equalsIgnoreCase(schemaId)) {
                throw new IllegalArgumentException(String.format("Realm '%s' is already provided by schema '%s'", realmTemplate, existingSchemaId));
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
                if (mergeRealmPath(realmTemplate, realmTemplateEntity.entity, rootSegment, tailPath, counter)) {
                    modified = true;
                }
            } else {
                Node firstSegment = firstLevel.getEndNode();

                Object existingPath = firstSegment.getProperty(PATH);

                if (existingPath.equals(rootSegment.path)) {
                    updateRealmPath(realmTemplate, rootSegment, tailPath, counter, firstSegment);
                } else {
                    throw new IllegalArgumentException(String.format("Realm %s can have only one root level : %s != %s", realmTemplate, existingPath, rootSegment.path));
                }
            }
        }

        if (realmTemplateEntity.wasCreated) {
            for (Relationship relationship : realmTemplateEntity.entity.getRelationships(PROVIDED, INCOMING)) {
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
        if (newPlanets.size() == 1) {
            PlanetNode newPlanet = Iterables.getOnlyElement(newPlanets);
            // Move all ne from old planets to new one
            Node newPlanetNode = planetFactory.getOrCreate(newPlanet.name, scope).entity;
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

    private ImmutableSet<Node> createRealmTemplates(Schema schema, ImmutableMap<String, CounterNode> counters) {
        Set<Node> realmTemplates = Sets.newHashSet();
        for (RealmNode realmNodeEntry : schema.realms.values()) {
            String realm = realmNodeEntry.name;
            UniqueEntity<Node> realmTemplateEntity = realmTemplateFactory.getOrCreateWithOutcome(NAME, realm);

            if (!realmTemplateEntity.wasCreated) {
                deleteTree(false, realmTemplateEntity.entity, OUTGOING, MEMDEXPATH);
                realmTemplateEntity.entity.getRelationships(PROVIDED, INCOMING).forEach(Relationship::delete);
            }

            createRealm(realmTemplateEntity, realmNodeEntry, counters);

            realmTemplates.add(realmTemplateEntity.entity);
        }
        return ImmutableSet.copyOf(realmTemplates);
    }

    private boolean mergeRealmPath(String realmTemplate,
                                   Node parentNode,
                                   RealmPathSegment segment,
                                   ImmutableList<RealmPathSegment> tail,
                                   CounterNode counter) {
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
            segmentNode = graphDb.createNode(Labels.SEGMENT);
            segmentNode.setProperty(PATH, segment.path);
            parentNode.createRelationshipTo(segmentNode, MEMDEXPATH);
            modified = true;
        }

        if (updateRealmPath(realmTemplate, segment, tail, counter, segmentNode)) {
            modified = true;
        }

        return modified;
    }

    private boolean updateRealmPath(String realmTemplate,
                                    RealmPathSegment segment,
                                    ImmutableList<RealmPathSegment> tail,
                                    CounterNode counter,
                                    Node segmentNode) {
        boolean modified = false;

        if (replaceAttributesRelationships(ImmutableSet.of(segment.keyAttribute), segmentNode)) {
            modified = true;
        }

        if (tail.isEmpty()) {
            if (appendCounterToSegment(realmTemplate, counter, segmentNode)) {
                modified = true;
            }
        } else {
            if (mergeRealmPath(realmTemplate, segmentNode, tail.get(0), tail.subList(1, tail.size()), counter)) {
                modified = true;
            }
        }
        return modified;
    }

    private void createRealm(UniqueEntity<Node> realmTemplateEntity, RealmNode realm, ImmutableMap<String, CounterNode> counters) {
        replaceAttributesRelationships(realm.attributes, realmTemplateEntity.entity);
        Node memdexPathNode = createMemdexTree(realm.name, realm.memdexPath, counters);
        realmTemplateEntity.entity.createRelationshipTo(memdexPathNode, MEMDEXPATH);
    }

    private Node createMemdexTree(String realmTemplate, MemdexPathNode memdexPathNode, ImmutableMap<String, CounterNode> counters) {
        Node segmentNode = graphDb.createNode(Labels.SEGMENT);
        segmentNode.setProperty(PATH, memdexPathNode.segment);

        updateCountersRelationships(realmTemplate, memdexPathNode, counters, segmentNode);

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
            Node childNode = createMemdexTree(realmTemplate, child, counters);
            segmentNode.createRelationshipTo(childNode, MEMDEXPATH);
        }

        return segmentNode;
    }

    private boolean appendCounterToSegment(String realmTemplate, CounterNode counter, Node segmentNode) {
        boolean modified = false;
        Relationship relationship = null;
        for (Relationship existingRelationship : segmentNode.getRelationships(INCOMING, PROVIDED)) {
            Node counterNode = existingRelationship.getStartNode();
            if (counter.name.equals(counterNode.getProperty(NAME).toString())) {
                relationship = existingRelationship;
                break;
            }
        }

        UniqueEntity<Node> counterEntity = counterNodeFactory.getOrCreateWithOutcome(NAME, counter.name);
        if (counterEntity.wasCreated) {
            modified = true;
            counterEntity.entity.setProperty(_TYPE, "counter");
            counterEntity.entity.setProperty("defaultAggregation", counter.defaultAggregation);
            if (counter.defaultValue == null) {
                counterEntity.entity.removeProperty("defaultValue");
            } else {
                counterEntity.entity.setProperty("defaultValue", counter.defaultValue);
            }
            counterEntity.entity.setProperty("valueType", counter.valueType);
            counterEntity.entity.setProperty("unit", counter.unit);
        }

        if (relationship == null) {
            relationship = counterEntity.entity.createRelationshipTo(segmentNode, PROVIDED);
            relationship.setProperty(GraphModelConstants.CONTEXT, realmTemplate);
            modified = true;
        } else {
            String existingContext = relationship.getProperty(GraphModelConstants.CONTEXT, "").toString();
            if (existingContext.equals(realmTemplate)) {
                relationship.setProperty(GraphModelConstants.CONTEXT, realmTemplate);
                modified = true;
            }
        }

        return modified;
    }

    private void updateCountersRelationships(String realmTemplate,
                                             MemdexPathNode memdexPath,
                                             ImmutableMap<String, CounterNode> counters,
                                             Node segmentNode) {
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
            relationshipTo.setProperty(GraphModelConstants.CONTEXT, realmTemplate);
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
