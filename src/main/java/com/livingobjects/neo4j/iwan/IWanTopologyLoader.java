package com.livingobjects.neo4j.iwan;

import au.com.bytecode.opencsv.CSVReader;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer.Context;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Booleans;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import com.livingobjects.neo4j.Neo4jLoadResult;
import com.livingobjects.neo4j.iwan.model.HeaderElement;
import com.livingobjects.neo4j.iwan.model.HeaderElement.Visitor;
import com.livingobjects.neo4j.iwan.model.MultiElementHeader;
import com.livingobjects.neo4j.iwan.model.NetworkElementFactory;
import com.livingobjects.neo4j.iwan.model.NetworkElementFactory.UniqueEntity;
import com.livingobjects.neo4j.iwan.model.SimpleElementHeader;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.iwan.model.HeaderElement.ELEMENT_SEPARATOR;
import static com.livingobjects.neo4j.iwan.model.IWanHelperConstants.*;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.*;

public final class IWanTopologyLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(IWanTopologyLoader.class);
    private static final int MAX_TRANSACTION_COUNT = 500;

    private final MetricRegistry metrics;
    private final GraphDatabaseService graphDb;
    private final NetworkElementFactory networkElementFactory;

    private final ImmutableMap<String, Node> attributeNodes;
    private final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    private final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    private final ImmutableList<String> scopes;
    private ImmutableMap<String, Set<String>> lineage;

    private final Map<String, ImmutableMultimap<String, Node>> planetsByClient = Maps.newHashMap();

    public IWanTopologyLoader(GraphDatabaseService graphDb, MetricRegistry metrics) {
        this.metrics = metrics;
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {

            this.networkElementFactory = NetworkElementFactory.build(graphDb);

            ImmutableMap.Builder<String, Node> attributesBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> childrenRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> parentRelationsBldr = ImmutableMap.builder();
            ImmutableList.Builder<String> scopesBldr = ImmutableList.builder();
            graphDb.findNodes(LABEL_ATTRIBUTE).forEachRemaining(n -> {
                String keytype = n.getProperty(_TYPE).toString();
                String key = keytype + KEYTYPE_SEPARATOR + n.getProperty(NAME).toString();
                attributesBldr.put(key, n);
                if (KEY_TYPES.contains(keytype)) {
                    ImmutableList.Builder<Relationship> crels = ImmutableList.builder();
                    ImmutableList.Builder<Relationship> prels = ImmutableList.builder();
                    n.getRelationships(Direction.INCOMING, LINK_PARENT).forEach(crels::add);
                    n.getRelationships(Direction.OUTGOING, LINK_PARENT).forEach(prels::add);
                    if (prels.build().isEmpty()) {
                        scopesBldr.add(key);
                    }
                    childrenRelationsBldr.put(key, crels.build());
                    parentRelationsBldr.put(key, prels.build());
                }
            });

            this.attributeNodes = attributesBldr.build();
            this.childrenRelations = childrenRelationsBldr.build();
            this.parentRelations = parentRelationsBldr.build();
            this.scopes = scopesBldr.build();
        }
    }

    public Neo4jLoadResult loadFromStream(InputStream is) throws IOException {
        CSVReader reader = new CSVReader(new InputStreamReader(is));
        IwanMappingStrategy strategy = IwanMappingStrategy.captureHeader(reader);

        String[] nextLine;

        int imported = 0;
        List<String[]> currentTransaction = Lists.newArrayListWithCapacity(MAX_TRANSACTION_COUNT);
        List<Long> errors = Lists.newArrayListWithExpectedSize(MAX_TRANSACTION_COUNT);

        Transaction tx = graphDb.beginTx();
        lineage = strategy.guessElementCreationStrategy(scopes, childrenRelations);
        ImmutableSet<String> startKeytypes = ImmutableSet.copyOf(
                scopes.stream().filter(strategy::hasKeyType).collect(Collectors.toSet()));

        while ((nextLine = reader.readNext()) != null) {
            ++imported;
            try {
                importLine(nextLine, startKeytypes, strategy);
                currentTransaction.add(nextLine);
                if (currentTransaction.size() >= MAX_TRANSACTION_COUNT) {
                    tx = renewTransaction(tx);
                    currentTransaction.clear();
                }
            } catch (IllegalArgumentException e) {
                tx = renewTransaction(tx, true);
                tx = reloadValidTransactionLines(tx, currentTransaction, startKeytypes, strategy);
                currentTransaction.clear();
                errors.add((long) imported);
                LOGGER.error(e.getLocalizedMessage());
                LOGGER.error("STACKTRACE", e);
                LOGGER.debug(Arrays.toString(nextLine));
            }
        }
        tx.success();
        tx.close();

        long created = imported - errors.size();
        return new Neo4jLoadResult(created, Longs.toArray(errors));
    }

    private Transaction renewTransaction(Transaction tx) {
        return renewTransaction(tx, false);
    }

    private Transaction renewTransaction(Transaction tx, boolean asFailure) {
        if (asFailure) {
            tx.failure();
        } else {
            tx.success();
        }
        tx.close();
        return graphDb.beginTx();
    }

    private Transaction reloadValidTransactionLines(
            Transaction tx, List<String[]> lines, ImmutableSet<String> startKeytypes, IwanMappingStrategy strategy) {
        if (!lines.isEmpty()) {
            lines.forEach(ct -> importLine(ct, startKeytypes, strategy));
            return renewTransaction(tx);
        }
        return tx;
    }

    private void importLine(String[] line, ImmutableSet<String> startKeytypes, IwanMappingStrategy strategy) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-importLine").time()) {
            // Create elements
            Map<String, Node> nodes = lineage.keySet().stream()
                    .map(key -> Maps.immutableEntry(key, createElement(strategy, line, key)))
                    .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

            createCrossAttributeLinks(line, strategy, nodes);

            // Create Connect link
            for (String keytype : nodes.keySet()) {
                int relCount = linkToParents(keytype, nodes);
                if (!startKeytypes.isEmpty() && !scopes.contains(keytype) && relCount <= 0) {
                    throw new IllegalStateException("No parent element found for type " + keytype);
                }
            }

            // Create planet link
            ImmutableMultimap<String, Node> planets;
            for (String keytype : startKeytypes) {
                String id = nodes.get(keytype).getProperty("id").toString();
                String name = keytype.substring(keytype.indexOf(KEYTYPE_SEPARATOR) + 1);
                planets = getPlanetsForClient(name + KEYTYPE_SEPARATOR + id);

                for (Entry<String, Node> entry : nodes.entrySet()) {
                    planets.get(entry.getKey()).forEach(n ->
                            createUniqueLink(entry.getValue(), n, Direction.OUTGOING, LINK_ATTRIBUTE));
                }
            }

            if (startKeytypes.isEmpty()) {
                planets = getGlobalPlanets();
                for (Entry<String, Node> entry : nodes.entrySet()) {
                    planets.get(entry.getKey()).forEach(n ->
                            createUniqueLink(entry.getValue(), n, Direction.OUTGOING, LINK_ATTRIBUTE));
                }
            }
        }
    }

    private void createCrossAttributeLinks(String[] line, IwanMappingStrategy strategy, Map<String, Node> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createCrossAttributeLinks").time()) {
            Map<String, Relationship> multiElementLinks = Maps.newHashMap();
            for (MultiElementHeader meHeader : strategy.getMultiElementHeader()) {
                String key = meHeader.elementName + ELEMENT_SEPARATOR + meHeader.targetElementName;
                Node fromNode = nodes.get(meHeader.elementName);
                Node toNode = nodes.get(meHeader.targetElementName);
                if (fromNode == null || toNode == null) {
                    continue;
                }

                Relationship relationship = multiElementLinks.computeIfAbsent(key,
                        k -> createUniqueLink(fromNode, toNode, Direction.OUTGOING, LINK_CROSS_ATTRIBUTE));
                relationship = persistElementProperty(meHeader, line, relationship);
                multiElementLinks.put(key, relationship);
            }
        }
    }

    private int linkToParents(String keytype, Map<String, Node> nodes) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-linkToParents").time()) {
            Node node = nodes.get(keytype);
            if (node == null) {
                return -1;
            }

            ImmutableList<Relationship> relationships = parentRelations.get(keytype);
            if (relationships == null || relationships.isEmpty()) {
                return -1;
            }

            int relCount = 0;
            for (Relationship relationship : relationships) {
                String toKeytype = relationship.getEndNode().getProperty(_TYPE).toString() + KEYTYPE_SEPARATOR +
                        relationship.getEndNode().getProperty(NAME).toString();
                Node parent = nodes.get(toKeytype);
                if (parent == null) {
                    continue;
                }
                ++relCount;
                createUniqueLink(node, parent, Direction.OUTGOING, LINK_CONNECT);
            }

            return relCount;
        }
    }

    private Relationship createUniqueLink(Node node, Node parent, Direction direction, RelationshipType linkType) {
        for (Relationship next : node.getRelationships(direction, linkType)) {
            if (next.getEndNode().equals(parent)) {
                return next;
            }
        }
        return node.createRelationshipTo(parent, linkType);
    }

    private Node createElement(IwanMappingStrategy strategy, String[] line, String elementName) {
        try (Context ignore = metrics.timer("IWanTopologyLoader-createElement").time()) {
            if (SCOPE_GLOBAL_ATTRIBUTE.equals(elementName)) {
                return graphDb.findNode(LABEL_SCOPE, "tag", SCOPE_GLOBAL_TAG);
            }

            Node elementAttNode = attributeNodes.get(elementName);
            if (elementAttNode == null) {
                throw new IllegalStateException("Unknown element type: '" + elementName + "' !");
            }
            boolean isOverridable = Boolean.parseBoolean(elementAttNode.getProperty(_OVERRIDABLE, Boolean.FALSE).toString());

            Set<String> todelete = parentRelations.get(elementName).stream()
                    .filter(r -> !CARDINALITY_MULTIPLE.equals(r.getProperty(CARDINALITY, "")))
                    .map(r -> r.getEndNode().getProperty(_TYPE).toString() + KEYTYPE_SEPARATOR + r.getEndNode().getProperty(NAME).toString())
                    .filter(strategy::hasKeyType)
                    .collect(Collectors.toSet());

            ImmutableCollection<HeaderElement> elementHeaders = strategy.getElementHeaders(elementName);
            HeaderElement tagHeader = elementHeaders.stream()
                    .filter(h -> TAG.equals(h.propertyName))
                    .findAny()
                    .orElseThrow(() -> new IllegalArgumentException(TAG + " not found for element " + elementName + " !"));

            String tag = line[tagHeader.index];

            UniqueEntity<Node> uniqueEntity = networkElementFactory.getOrCreateWithOutcome(TAG, tag);
            Node elementNode = uniqueEntity.entity;

            if (uniqueEntity.wasCreated) {
                if (scopes.contains(elementName)) {
                    elementNode.addLabel(LABEL_SCOPE);
                }
                elementNode.setProperty(_TYPE, elementName);
                if (isOverridable) {
                    elementNode.setProperty(_SCOPE, SCOPE_GLOBAL_TAG);
                }
            } else {
                elementNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());

                elementNode.getRelationships(Direction.OUTGOING, LINK_CONNECT).forEach(r -> {
                    String type = r.getEndNode().getProperty(_TYPE, SCOPE_GLOBAL_ATTRIBUTE).toString();
                    if (todelete.contains(type)) {
                        r.delete();
                    }
                });
            }

            elementNode = persistElementProperties(line, elementHeaders, elementNode);

            return elementNode;
        }
    }

    private static Node persistElementProperties(String[] line, ImmutableCollection<HeaderElement> elementHeaders, Node elementNode) {
        elementHeaders.stream()
                .filter(h -> !TAG.equals(h.propertyName))
                .forEach(h -> h.visit(new Visitor<Void>() {
                    @Override
                    public Void visitSimple(SimpleElementHeader header) {
                        persistElementProperty(header, line, elementNode);
                        return null;
                    }

                    @Override
                    public Void visitMulti(MultiElementHeader header) {
                        return null;
                    }
                }));

        return elementNode;
    }

    private static <T extends PropertyContainer> T persistElementProperty(HeaderElement header, final String[] line, final T elementNode) {
        Object value;
        try {
            switch (header.type) {
                case BOOLEAN:
                    value = (header.isArray) ?
                            Booleans.toArray(JSON_MAPPER.readValue(line[header.index], BOOLEAN_LIST_TYPE))
                            : Boolean.parseBoolean(line[header.index]);
                    break;
                case NUMBER:
                    value = (header.isArray) ?
                            Doubles.toArray(JSON_MAPPER.readValue(line[header.index], DOUBLE_LIST_TYPE))
                            : Doubles.tryParse(line[header.index]);
                    break;
                default:
                    value = (header.isArray) ?
                            Iterables.toArray(JSON_MAPPER.readValue(line[header.index], STRING_LIST_TYPE), String.class)
                            : line[header.index];
            }
        } catch (Exception ignored) {
            LOGGER.debug("Unable to parse value " + line[header.index] + " as " + header.type);
            value = line[header.index];
        }
        if (value != null) {
            elementNode.setProperty(header.propertyName, value);
        } else {
            elementNode.removeProperty(header.propertyName);
        }
        return elementNode;
    }

    private ImmutableMultimap<String, Node> getPlanetsForClient(String clientAttribute) {
        ImmutableMultimap<String, Node> planets = planetsByClient.get(clientAttribute);
        if (planets == null) {
            try (Context ignore = metrics.timer("IWanTopologyLoader-getPlanetsForClient").time()) {
                Node attClientNode = attributeNodes.get(clientAttribute);
                Builder<String, Node> bldr = ImmutableMultimap.builder();
                attClientNode.getRelationships(Direction.INCOMING, LINK_ATTRIBUTE)
                        .forEach(pr -> collectPlanetConsumer(pr, bldr));
                planets = bldr.build();
                planetsByClient.put(clientAttribute, planets);
            }
        }

        return planets;
    }

    private ImmutableMultimap<String, Node> getGlobalPlanets() {
        ImmutableMultimap<String, Node> planets = planetsByClient.get(Character.toString(KEYTYPE_SEPARATOR));
        if (planets == null) {
            try (Context ignore = metrics.timer("IWanTopologyLoader-getGlobalPlanets").time()) {
                Node scopeGlobal = attributeNodes.get(SCOPE_GLOBAL_ATTRIBUTE);
                Builder<String, Node> bldr = ImmutableMultimap.builder();
                scopeGlobal.getRelationships(Direction.INCOMING, LINK_PARENT).forEach(r -> {
                    Node keyAttributeNode = r.getStartNode();
                    if (!KEY_TYPES.contains(keyAttributeNode.getProperty(_TYPE).toString())) {
                        return;
                    }

                    keyAttributeNode.getRelationships(Direction.INCOMING, LINK_ATTRIBUTE)
                            .forEach(pr -> collectPlanetConsumer(pr, bldr));
                });

                planets = bldr.build();
                planetsByClient.put(Character.toString(KEYTYPE_SEPARATOR), planets);
            }
        }

        return planets;
    }

    private void collectPlanetConsumer(Relationship pr, Builder<String, Node> bldr) {
        Node planet = pr.getStartNode();
        if (!planet.hasLabel(LABEL_PLANET)) {
            return;
        }

        planet.getRelationships(Direction.OUTGOING, LINK_ATTRIBUTE)
                .forEach(l -> getRelationshipConsumer(l, bldr));
    }

    private Builder<String, Node> getRelationshipConsumer(Relationship l, Builder<String, Node> bldr) {
        Node planet = l.getStartNode();
        Node attribute = l.getEndNode();
        if (attribute.hasLabel(LABEL_ATTRIBUTE)) {
            String type = attribute.getProperty(_TYPE).toString();
            if (KEY_TYPES.contains(type)) {
                bldr.put(type + KEYTYPE_SEPARATOR + attribute.getProperty(NAME), planet);
            }
        }
        return bldr;
    }
}
