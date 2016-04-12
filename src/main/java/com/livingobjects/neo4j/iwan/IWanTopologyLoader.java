package com.livingobjects.neo4j.iwan;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableMultimap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.Neo4jResult;
import com.livingobjects.neo4j.iwan.model.HeaderElement;
import com.livingobjects.neo4j.iwan.model.NetworkElementFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryStatistics;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.UniqueFactory.UniqueEntity;
import org.neo4j.graphdb.index.UniqueFactory.UniqueNodeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.livingobjects.cosmos.shared.model.GraphLinkProperties.CARDINALITY;
import static com.livingobjects.cosmos.shared.model.GraphNodeProperties.*;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.*;

public final class IWanTopologyLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(IWanTopologyLoader.class);
    private static final ImmutableSet<String> KEY_TYPES = ImmutableSet.of("cluster", "neType", "labelType", "scope");
    private static final int MAX_TRANSACTION_COUNT = 500;
    private static final String CARDINALITY_MULTIPLE = "0..n";

    private final GraphDatabaseService graphDb;
    private final UniqueNodeFactory networkElementFactory;

    private final ImmutableMap<String, Node> attributeNodes;
    private final ImmutableMap<String, ImmutableList<Relationship>> childrenRelations;
    private final ImmutableMap<String, ImmutableList<Relationship>> parentRelations;
    private final ImmutableList<String> scopes;
    private ImmutableMap<String, List<String>> lineage;

    private final Map<String, ImmutableMultimap<String, Node>> planetsByClient = Maps.newHashMap();

    public IWanTopologyLoader(GraphDatabaseService graphDb) {
        Stopwatch sw = Stopwatch.createStarted();
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {

            this.networkElementFactory = NetworkElementFactory.build(graphDb);

            ImmutableMap.Builder<String, Node> attributesBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> childrenRelationsBldr = ImmutableMap.builder();
            ImmutableMap.Builder<String, ImmutableList<Relationship>> parentRelationsBldr = ImmutableMap.builder();
            ImmutableList.Builder<String> scopesBldr = ImmutableList.builder();
            graphDb.findNodes(LABEL_ATTRIBUTE).forEachRemaining(n -> {
                String keytype = n.getProperty(_TYPE).toString();
                String key = keytype + ':' + n.getProperty(NAME).toString();
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
        LOGGER.info("IWanTopologyLoader: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
    }

    public Neo4jResult loadFromStream(InputStream is) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        CSVReader reader = new CSVReader(new InputStreamReader(is));
        IwanMappingStrategy strategy = IwanMappingStrategy.captureHeader(reader);

        String[] nextLine;

        int imported = 0;
        List<String[]> currentTransaction = Lists.newArrayListWithCapacity(MAX_TRANSACTION_COUNT);
        List<Integer> errors = Lists.newArrayListWithExpectedSize(MAX_TRANSACTION_COUNT);

        Transaction tx = graphDb.beginTx();
        lineage = strategy.guessElementCreationStrategy(scopes, childrenRelations);
        ImmutableSet<String> startKeytypes = ImmutableSet.copyOf(
                scopes.stream().filter(strategy::isScope).collect(Collectors.toSet()));

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
                errors.add(imported);
                LOGGER.error(e.getLocalizedMessage());
                LOGGER.error("STACKTRACE", e);
                LOGGER.debug(Arrays.toString(nextLine));
            }
        }
        tx.close();

        final int created = imported - errors.size();
        final int error = errors.size();
        LOGGER.info("loadFromStream: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
        return new Neo4jResult(new QueryStatistics() {
            @Override
            public int getNodesCreated() {
                return created;
            }

            @Override
            public int getNodesDeleted() {
                return error;
            }

            @Override
            public int getRelationshipsCreated() {
                return 0;
            }

            @Override
            public int getRelationshipsDeleted() {
                return 0;
            }

            @Override
            public int getPropertiesSet() {
                return 0;
            }

            @Override
            public int getLabelsAdded() {
                return 0;
            }

            @Override
            public int getLabelsRemoved() {
                return 0;
            }

            @Override
            public int getIndexesAdded() {
                return 0;
            }

            @Override
            public int getIndexesRemoved() {
                return 0;
            }

            @Override
            public int getConstraintsAdded() {
                return 0;
            }

            @Override
            public int getConstraintsRemoved() {
                return 0;
            }

            @Override
            public boolean containsUpdates() {
                return true;
            }
        });
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

        // Create elements
        Map<String, Node> nodes = lineage.keySet().stream()
                .map(key -> Maps.immutableEntry(key, createElement(strategy, line, key)))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

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
            String name = keytype.substring(keytype.indexOf(':') + 1);
            planets = getPlanetsForClient(name + ':' + id);

            for (Entry<String, Node> entry : nodes.entrySet()) {
                planets.get(entry.getKey()).forEach(n ->
                        createUniqueLink(entry.getValue(), n, Direction.OUTGOING, LINK_ATTRIBUTE));
            }
        }

        if (startKeytypes.isEmpty()) {
            planets = getPlanets();
            for (Entry<String, Node> entry : nodes.entrySet()) {
                planets.get(entry.getKey()).forEach(n ->
                        createUniqueLink(entry.getValue(), n, Direction.OUTGOING, LINK_ATTRIBUTE));
            }
        }

    }

    private int linkToParents(String keytype, Map<String, Node> nodes) {
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
            String toKeytype = relationship.getEndNode().getProperty(_TYPE).toString() + ':' +
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

    private Relationship createUniqueLink(Node node, Node parent, Direction outgoing, RelationshipType linkConnect) {
        Iterator<Relationship> it = node.getRelationships(outgoing, linkConnect).iterator();
        while (it.hasNext()) {
            Relationship next = it.next();
            if (next.getEndNode().equals(parent)) {
                return next;
            }
        }

        return node.createRelationshipTo(parent, linkConnect);
    }

    private Node createElement(IwanMappingStrategy strategy, String[] line, String elementName) {
        Set<String> todelete = parentRelations.get(elementName).stream()
                .filter(r -> !CARDINALITY_MULTIPLE.equals(r.getProperty(CARDINALITY)))
                .map(r -> r.getEndNode().getProperty(_TYPE).toString() + ':' + r.getEndNode().getProperty(NAME).toString())
                .collect(Collectors.toSet());

        ImmutableCollection<HeaderElement> elementHeaders = strategy.getElementHeaders(elementName);
        HeaderElement tagHeader = elementHeaders.stream()
                .filter(h -> TAG.equals(h.propertyName))
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException(TAG + " not found for element " + elementName + " !"));
        String tag = line[tagHeader.index];
        UniqueEntity<Node> uniqueEntity = networkElementFactory.getOrCreateWithOutcome(TAG, tag);
        Node elementNode = uniqueEntity.entity();

        if (uniqueEntity.wasCreated()) {
            if (scopes.contains(elementName)) {
                elementNode.addLabel(LABEL_SCOPE);
            }
            elementNode.setProperty(_TYPE, elementName);
        } else {
            elementNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());

            elementNode.getRelationships(Direction.OUTGOING, LINK_CONNECT).forEach(r -> {
                if (todelete.contains(r.getEndNode().getProperty(_TYPE).toString())) {
                    r.delete();
                }
            });
        }

        elementHeaders.stream()
                .filter(h -> !TAG.equals(h.propertyName))
                .forEach(h -> elementNode.setProperty(h.propertyName, line[h.index]));

        return elementNode;
    }

    private ImmutableMultimap<String, Node> getPlanetsForClient(String clientAttribute) {
        Stopwatch sw = Stopwatch.createStarted();
        ImmutableMultimap<String, Node> planets = planetsByClient.get(clientAttribute);
        if (planets == null) {
            Node attClientNode = attributeNodes.get(clientAttribute);
            ImmutableMultimap.Builder<String, Node> bldr = ImmutableMultimap.builder();
            attClientNode.getRelationships(Direction.INCOMING, LINK_ATTRIBUTE).forEach(r -> {
                Node planet = r.getStartNode();
                if (!planet.hasLabel(LABEL_PLANET)) {
                    return;
                }

                planet.getRelationships(Direction.OUTGOING, LINK_ATTRIBUTE)
                        .forEach(l -> getRelationshipConsumer(l, bldr, planet));

            });
            planets = bldr.build();
            planetsByClient.put(clientAttribute, planets);
        }

        LOGGER.info("getPlanetsForClient: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
        return planets;
    }

    private ImmutableMultimap<String, Node> getPlanets() {
        Stopwatch sw = Stopwatch.createStarted();
        ImmutableMultimap<String, Node> planets = planetsByClient.get(":");
        if (planets == null) {
            ImmutableMultimap.Builder<String, Node> bldr = ImmutableMultimap.builder();
            graphDb.findNodes(LABEL_PLANET).forEachRemaining(planet -> {
                if (!planet.hasLabel(LABEL_PLANET)) {
                    return;
                }

                planet.getRelationships(Direction.OUTGOING, LINK_ATTRIBUTE)
                        .forEach(l -> getRelationshipConsumer(l, bldr, planet));

            });
            planets = bldr.build();
            planetsByClient.put(":", planets);
        }

        LOGGER.info("getPlanets: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
        return planets;
    }

    private Builder<String, Node> getRelationshipConsumer(Relationship l, Builder<String, Node> bldr, Node planet) {
        Node attribute = l.getEndNode();
        if (attribute.hasLabel(LABEL_ATTRIBUTE)) {
            String type = attribute.getProperty(_TYPE).toString();
            if (KEY_TYPES.contains(type)) {
                bldr.put(type + ':' + attribute.getProperty(NAME), planet);
            }
        }
        return bldr;
    }
}
