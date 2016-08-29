package com.livingobjects.neo4j;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.iwan.model.UniqueElementFactory;
import com.livingobjects.neo4j.iwan.model.UniqueElementFactory.UniqueEntity;
import com.livingobjects.neo4j.iwan.model.schema.Attribute;
import com.livingobjects.neo4j.iwan.model.schema.Counter;
import com.livingobjects.neo4j.iwan.model.schema.MemdexPathRelation;
import com.livingobjects.neo4j.iwan.model.schema.Planet;
import com.livingobjects.neo4j.iwan.model.schema.RealmPathElement;
import com.livingobjects.neo4j.iwan.model.schema.RealmTemplate;
import com.livingobjects.neo4j.iwan.model.schema.Schema;
import com.livingobjects.neo4j.iwan.model.schema.SchemaResult;
import com.livingobjects.neo4j.iwan.model.schema.SchemaVersion;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_NETWORK_ELEMENT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_SCOPE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LINK_ATTRIBUTE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LINK_MEMDEXPATH;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LINK_PROVIDED;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.TAG;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.UPDATED_AT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants._TYPE;

@Path("/schema")
public final class CustomerSchemaExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomerSchemaExtension.class);
    public static final String SCHEMA_VERSION = "schemaVersion";
    public static final String CUSTOMER_NAME = "client";
    public static final String CUSTOMER_TYPE = "cluster";
    public static final String DEFAULT_SCHEMA_VERSION = "0";

    private final GraphDatabaseService graphDb;

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();

    private final UniqueElementFactory attributeFactory;

    private final UniqueElementFactory networkElementFactory;

    private final UniqueElementFactory planetFactory;

    private final UniqueElementFactory counterFactory;

    public CustomerSchemaExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        attributeFactory = UniqueElementFactory.attributeFactory(graphDb);
        networkElementFactory = UniqueElementFactory.networkElementFactory(graphDb);
        planetFactory = UniqueElementFactory.planetFactory(graphDb);
        counterFactory = UniqueElementFactory.counterFactory(graphDb);
    }

    @POST
    public Response updateSchemes(InputStream request) throws IOException, ServletException {
        try {
            List<Schema> schemes = JSON_MAPPER.readValue(request, new TypeReference<List<Schema>>() {
            });
            AtomicInteger updated = new AtomicInteger(0);

            Iterable<List<Schema>> partition = Iterables.partition(schemes, 20);
            for (List<Schema> batch : partition) {
                try (Transaction tx = graphDb.beginTx()) {
                    Multimap<String, Node> globalPlanets = HashMultimap.create();
                    for (Schema schema : batch) {
                        Optional<Multimap<String, Node>> result = applySchema(schema);
                        if (result.isPresent()) {
                            updated.incrementAndGet();
                            globalPlanets.putAll(result.get());
                        }
                    }
                    linkGlobalPlanets(globalPlanets);
                    tx.success();
                    LOGGER.info("Flushing {} schemes...", batch.size());
                }
            }
            String json = JSON_MAPPER.writeValueAsString(new SchemaResult(updated.get()));
            return Response.ok().entity(json).type(MediaType.APPLICATION_JSON).build();
        } catch (Throwable e) {
            LOGGER.error("Unable to update schemes", e);
            return Response.serverError().entity(e).type(MediaType.APPLICATION_JSON).build();
        }
    }

    private void linkGlobalPlanets(Multimap<String, Node> globalPlanets) {
        for (Map.Entry<String, Collection<Node>> entry : globalPlanets.asMap().entrySet()) {
            String keyAttribute = entry.getKey();
            ResourceIterator<Node> nodes = graphDb.findNodes(LABEL_NETWORK_ELEMENT, _TYPE, keyAttribute);
            while (nodes.hasNext()) {
                Node node = nodes.next();
                for (Node planetNode : entry.getValue()) {
                    node.createRelationshipTo(planetNode, LINK_ATTRIBUTE);
                }
            }
        }
        LOGGER.debug("\t{} global planet(s) have been linked to topology elements.", globalPlanets.size());
    }

    private Optional<Multimap<String, Node>> applySchema(Schema schema) {
        UniqueEntity<Node> uniqueEntity = attributeFactory.getOrCreateWithOutcome(_TYPE, CUSTOMER_NAME, NAME, schema.customerId);
        Node customerNode = uniqueEntity.entity;
        boolean updateVersion;
        if (uniqueEntity.wasCreated) {
            updateVersion = true;
        } else {
            SchemaVersion schemaVersion = getSchemaVersion(customerNode);
            updateVersion = schemaVersion.compareTo(schema.version) < 0;
        }
        if (updateVersion) {
            customerNode.setProperty(SCHEMA_VERSION, schema.version.toString());
            customerNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
            LOGGER.debug("Applying template schemaVersion=[{}] for customer {}", schema.version, schema.customerId);
            mergeScope(schema);
            Set<Node> context = setupContext(schema.attributes);
            Multimap<String, Node> globalPlanets = setupPlanets(context, schema.planets);
            globalPlanets.putAll(setupRealms(context, schema.realms));
            return Optional.of(globalPlanets);
        } else {
            return Optional.empty();
        }
    }

    private Multimap<String, Node> setupRealms(Set<Node> context, Set<RealmTemplate> realms) {
        Multimap<String, Node> globalPlanets = HashMultimap.create();
        int realmMerged = 0;
        for (RealmTemplate realm : realms) {
            UniqueEntity<Node> uniqueEntity = attributeFactory.getOrCreateWithOutcome(_TYPE, "realm", NAME, realm.name);
            Node realmNode = uniqueEntity.entity;
            Set<Counter> counters = realm.pathElements.stream()
                    .flatMap(p -> p.counters.stream())
                    .collect(Collectors.toSet());
            Iterable<Relationship> relationships = realmNode.getRelationships(Direction.INCOMING, LINK_PROVIDED);
            for (Relationship relationship : relationships) {
                relationship.delete();
            }
            Map<String, Node> planetsNodeByName = Maps.newHashMap();
            for (RealmPathElement pathElement : realm.pathElements) {
                UniqueEntity<Node> planetUniqueEntity = setupPlanet(context, pathElement.planet);
                Node planetNode = planetUniqueEntity.entity;
                planetsNodeByName.put(pathElement.planet.name, planetNode);
                if (planetUniqueEntity.wasCreated) {
                    if (pathElement.planet.global) {
                        globalPlanets.put(pathElement.planet.keyAttribute.toString(), planetNode);
                    }
                }
                Iterable<Relationship> providedRelationships = planetNode.getRelationships(Direction.INCOMING, LINK_PROVIDED);
                for (Relationship relationship : providedRelationships) {
                    relationship.delete();
                }
                for (Counter counter : counters) {
                    UniqueEntity<Node> counterEntity = counterFactory.getOrCreateWithOutcome("name", counter.name, "context", counter.context);
                    Node counterNode = counterEntity.entity;
                    if (!counterEntity.wasCreated) {
                        counterNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
                    }
                    counterNode.setProperty("defaultAggregation", counter.defaultAggregation);
                    counterNode.setProperty("valueType", counter.valueType);
                    counterNode.setProperty("unit", counter.unit);
                    if (counter.defaultValue != null) {
                        counterNode.setProperty("defaultValue", counter.defaultValue);
                    } else {
                        counterNode.removeProperty("defaultValue");
                    }
                    counterNode.createRelationshipTo(realmNode, LINK_PROVIDED);
                    counterNode.createRelationshipTo(planetNode, LINK_PROVIDED);
                }
            }
            for (MemdexPathRelation memdexPath : realm.memdexPaths) {
                Node startPlanet = planetsNodeByName.get(memdexPath.start);
                Iterable<Relationship> mdxPathRelationships = startPlanet.getRelationships(Direction.OUTGOING, LINK_MEMDEXPATH);
                for (Relationship relationship : mdxPathRelationships) {
                    relationship.delete();
                }
            }
            for (MemdexPathRelation memdexPath : realm.memdexPaths) {
                Node startPlanet = planetsNodeByName.get(memdexPath.start);
                Node endPlanet = planetsNodeByName.get(memdexPath.end);
                startPlanet.createRelationshipTo(endPlanet, LINK_MEMDEXPATH);
            }
            realmMerged++;
        }
        LOGGER.debug("\t{} realm(s) updated.", realmMerged);
        return globalPlanets;
    }

    private Multimap<String, Node> setupPlanets(Set<Node> context, Set<Planet> planets) {
        Multimap<String, Node> globalPlanets = HashMultimap.create();
        int planetsCreated = 0;
        for (Planet planet : planets) {
            UniqueEntity<Node> uniqueEntity = setupPlanet(context, planet);
            Node planetNode = uniqueEntity.entity;
            if (uniqueEntity.wasCreated) {
                planetsCreated++;
                if (planet.global) {
                    globalPlanets.put(planet.keyAttribute.toString(), planetNode);
                }
            }
        }
        LOGGER.debug("\t{} planet(s) created.", planetsCreated);
        return globalPlanets;
    }

    private UniqueEntity<Node> setupPlanet(Set<Node> context, Planet planet) {
        UniqueEntity<Node> uniqueEntity = planetFactory.getOrCreateWithOutcome(NAME, planet.name);
        if (uniqueEntity.wasCreated) {
            Node planetNode = uniqueEntity.entity;
            Set<Node> planetAttributes = Sets.newHashSet(context);
            planetAttributes.add(getOrCreateAttribute(planet.keyAttribute));
            for (Attribute attribute : planet.attributes) {
                Node node = getOrCreateAttribute(attribute);
                planetAttributes.add(node);
            }
            for (Node node : planetAttributes) {
                Iterable<Relationship> relationships = planetNode.getRelationships(Direction.OUTGOING, LINK_ATTRIBUTE);
                for (Relationship relationship : relationships) {
                    relationship.delete();
                }
                planetNode.createRelationshipTo(node, LINK_ATTRIBUTE);
            }
        }
        return uniqueEntity;
    }

    private Node getOrCreateAttribute(Attribute attribute) {
        return attributeFactory.getOrCreateWithOutcome(_TYPE, attribute.name, NAME, attribute.value).entity;
    }

    private Set<Node> setupContext(Set<Attribute> attributes) {
        Set<Node> context = Sets.newHashSet();
        for (Attribute attribute : attributes) {
            Node node = getOrCreateAttribute(attribute);
            context.add(node);
        }
        LOGGER.debug("\t{} context attribute(s) merged.", attributes);
        return context;
    }

    private void mergeScope(Schema schema) {
        UniqueEntity<Node> uniqueEntity = networkElementFactory.getOrCreateWithOutcome(TAG, schema.scope);
        Node node = uniqueEntity.entity;
        if (uniqueEntity.wasCreated) {
            node.setProperty(_TYPE, CUSTOMER_TYPE);
            node.addLabel(LABEL_SCOPE);
        } else {
            node.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
        }
        node.setProperty("id", schema.customerId);
        if (uniqueEntity.wasCreated) {
            LOGGER.debug("\tScope element {} created.", schema.scope);
        } else {
            LOGGER.debug("\tScope element {} updated.", schema.scope);
        }
    }

    public SchemaVersion getSchemaVersion(Node customerNode) {
        Object schemaVersionProperty = customerNode.getProperty(SCHEMA_VERSION, DEFAULT_SCHEMA_VERSION);
        if (schemaVersionProperty != null) {
            return SchemaVersion.of(schemaVersionProperty.toString());
        } else {
            return SchemaVersion.of(0);
        }
    }

}