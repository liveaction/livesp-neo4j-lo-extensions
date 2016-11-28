package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.iwan.model.IwanModelConstants;
import com.livingobjects.neo4j.iwan.model.UniqueEntity;
import com.livingobjects.neo4j.iwan.model.schema.factories.AttributeFactory;
import com.livingobjects.neo4j.iwan.model.schema.factories.CounterFactory;
import com.livingobjects.neo4j.iwan.model.schema.factories.PlanetFactory;
import com.livingobjects.neo4j.iwan.model.schema.factories.ScopeNetworkElementFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_ELEMENT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_NETWORK_ELEMENT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_SCOPE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LINK_ATTRIBUTE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LINK_EXTEND;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LINK_MEMDEXPATH;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LINK_PROVIDED;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.SCOPE_SP_TAG;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.UPDATED_AT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants._SCOPE;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants._TYPE;

public final class IWanSchemasLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(IWanSchemasLoader.class);
    private static final String SCHEMA_VERSION = "schemaVersion";
    private static final String CUSTOMER_NAME = "client";
    private static final String CUSTOMER_TYPE = "cluster";
    private static final String DEFAULT_SCHEMA_VERSION = "0";

    private final GraphDatabaseService graphDb;

    private final AttributeFactory attributeFactory;

    private final ScopeNetworkElementFactory scopeNetworkElementFactory;

    private final PlanetFactory planetFactory;

    private final CounterFactory counterFactory;

    public IWanSchemasLoader(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
        attributeFactory = new AttributeFactory(graphDb);
        scopeNetworkElementFactory = new ScopeNetworkElementFactory(graphDb);
        planetFactory = new PlanetFactory(graphDb);
        counterFactory = new CounterFactory(graphDb);
    }

    public int load(List<Schema> batch) {
        int count = 0;
        Multimap<String, Node> globalPlanets = HashMultimap.create();
        for (Schema schema : batch) {
            Optional<Multimap<String, Node>> result = applySchema(schema);
            if (result.isPresent()) {
                count++;
                globalPlanets.putAll(result.get());
            }
        }
        linkGlobalPlanets(globalPlanets);
        return count;
    }

    private void linkGlobalPlanets(Multimap<String, Node> globalPlanets) {
        for (Map.Entry<String, Collection<Node>> entry : globalPlanets.asMap().entrySet()) {
            String keyAttribute = entry.getKey();
            // Load all global NetworkElements of the corresponding keyAttribute
            ResourceIterator<Node> nodes = graphDb.findNodes(LABEL_NETWORK_ELEMENT, _TYPE, keyAttribute);
            while (nodes.hasNext()) {
                Node node = nodes.next();
                // Check if the node has been overriden by SP
                Optional<Node> spOverriden = isOverridenBySP(node);
                Node nodeToLink = spOverriden.orElse(node);
                for (Node planetNode : entry.getValue()) {
                    // link only global NetworkElements and sp overriden Elements
                    nodeToLink.createRelationshipTo(planetNode, LINK_ATTRIBUTE);
                }
            }
        }
        LOGGER.debug("\t{} global planet(s) have been linked to topology elements.", globalPlanets.size());
    }

    private Optional<Node> isOverridenBySP(Node node) {
        Iterable<Relationship> relationships = node.getRelationships(Direction.INCOMING, LINK_EXTEND);
        for (Relationship relationship : relationships) {
            Node startNode = relationship.getStartNode();
            Object scopeProperty = startNode.getProperty(_SCOPE, SCOPE_SP_TAG);
            if (SCOPE_SP_TAG.equals(scopeProperty.toString())) {
                return Optional.of(startNode);
            }
        }
        return Optional.empty();
    }

    private Optional<Multimap<String, Node>> applySchema(Schema schema) {
        UniqueEntity<Node> uniqueEntity = attributeFactory.getOrCreate(CUSTOMER_NAME + ':' + schema.customerId);
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
            UniqueEntity<Node> uniqueEntity = attributeFactory.getOrCreate("realm" + ':' + realm.name);
            Node realmNode = uniqueEntity.entity;
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
                for (Counter counter : pathElement.counters) {
                    UniqueEntity<Node> counterEntity = counterFactory.getOrCreate(counter.context + '@' + counter.name);
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
        UniqueEntity<Node> uniqueEntity = planetFactory.getOrCreate(planet.name);
        Node planetNode = uniqueEntity.entity;
        if (!uniqueEntity.wasCreated) {
            planetNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
        }
        planetNode.setProperty("path", planet.path);
        Set<Node> planetAttributes = Sets.newHashSet(context);
        Attribute planetKeyAttribute = planet.keyAttribute;
        Node planetKeyAttributeNode = attributeFactory.getOrCreate(planetKeyAttribute.type + ':' + planetKeyAttribute.name).entity;
        for (Attribute attribute : planet.attributes) {
            Node node = attributeFactory.getOrCreate(attribute.type + ':' + attribute.name).entity;
            planetAttributes.add(node);
        }
        Iterable<Relationship> relationships = planetNode.getRelationships(Direction.OUTGOING, LINK_ATTRIBUTE);
        for (Relationship relationship : relationships) {
            relationship.delete();
        }
        Relationship relationshipTo = planetNode.createRelationshipTo(planetKeyAttributeNode, LINK_ATTRIBUTE);
        if (planetKeyAttribute.specializer != null) {
            relationshipTo.setProperty("specializer", planetKeyAttribute.specializer);
        }
        for (Node node : planetAttributes) {
            planetNode.createRelationshipTo(node, LINK_ATTRIBUTE);
        }
        return uniqueEntity;
    }

    private Set<Node> setupContext(Set<Attribute> attributes) {
        Set<Node> context = Sets.newHashSet();
        for (Attribute attribute : attributes) {
            Node node = attributeFactory.getOrCreate(attribute.type + ':' + attribute.name).entity;
            context.add(node);
        }
        LOGGER.debug("\t{} context attribute(s) merged.", attributes);
        return context;
    }

    private void mergeScope(Schema schema) {
        UniqueEntity<Node> uniqueEntity = scopeNetworkElementFactory.getOrCreate(schema.scope);
        Node node = uniqueEntity.entity;
        if (uniqueEntity.wasCreated) {
            node.setProperty(_TYPE, CUSTOMER_TYPE + ':' + CUSTOMER_NAME);
            node.addLabel(LABEL_ELEMENT);
            node.addLabel(LABEL_SCOPE);
            UniqueEntity<Node> parentScope = scopeNetworkElementFactory.getOrCreate(IwanModelConstants.SCOPE_SP_TAG);
            parentScope.entity.createRelationshipTo(node, IwanModelConstants.LINK_CONNECT);
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
