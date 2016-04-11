package com.livingobjects.neo4j.iwan;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.Neo4jResult;
import com.livingobjects.neo4j.iwan.model.NetworkElementFactory;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryStatistics;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.livingobjects.cosmos.shared.model.GraphNodeProperties.*;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.*;

public final class IWanTopologyLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(IWanTopologyLoader.class);
    private static final ImmutableSet<String> KEY_TYPES = ImmutableSet.of("cluster", "neType");
    private static final int MAX_TRANSACTION_COUNT = 500;
    private final GraphDatabaseService graphDb;
    private final UniqueNodeFactory networkElementFactory;

    private ImmutableMap<String, Node> attributeNodes;

    private final Map<String, ImmutableMultimap<String, Node>> planetsByClient = Maps.newHashMap();

    public IWanTopologyLoader(GraphDatabaseService graphDb) {
        Stopwatch sw = Stopwatch.createStarted();
        this.graphDb = graphDb;
        try (Transaction ignore = graphDb.beginTx()) {

            this.networkElementFactory = NetworkElementFactory.build(graphDb);

            ImmutableMap.Builder<String, Node> attributesBldr = ImmutableMap.builder();
            graphDb.findNodes(LABEL_ATTRIBUTE).forEachRemaining(n ->
                    attributesBldr.put(n.getProperty(_TYPE).toString() + ':' + n.getProperty(NAME).toString(), n));
            this.attributeNodes = attributesBldr.build();
        }
        LOGGER.info("IWanTopologyLoader: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
    }

    public Neo4jResult loadFromStream(InputStream is) throws IOException {
        Stopwatch sw = Stopwatch.createStarted();
        CSVReader reader = new CSVReader(new InputStreamReader(is));
        IwanMappingStrategy strategy = IwanMappingStrategy.captureHeader(reader);

        String[] nextLine = null;

        int imported = 0;
        List<String[]> currentTransaction = Lists.newArrayListWithCapacity(MAX_TRANSACTION_COUNT);
        List<String[]> errors = Lists.newArrayListWithExpectedSize(MAX_TRANSACTION_COUNT);

        Transaction tx = graphDb.beginTx();
        while ((nextLine = reader.readNext()) != null) {
            try {
                importLine(nextLine, strategy);
                currentTransaction.add(nextLine);
                if (currentTransaction.size() >= MAX_TRANSACTION_COUNT) {
                    tx.success();
                    tx.close();
                    tx = graphDb.beginTx();
                    currentTransaction.clear();
                }
                ++imported;
            } catch (IllegalArgumentException e) {
                tx.failure();
                tx.close();
                errors.add(nextLine);
                LOGGER.error(e.getLocalizedMessage());
                LOGGER.error("STACKTRACE", e);
                LOGGER.debug(Arrays.toString(nextLine));
                reloadValidTransactionLines(currentTransaction, strategy);
                currentTransaction.clear();
                tx = graphDb.beginTx();
            }
        }
        tx.close();

        final int created = imported;
        LOGGER.info("loadFromStream: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
        return new Neo4jResult(new QueryStatistics() {
            @Override
            public int getNodesCreated() {
                return created;
            }

            @Override
            public int getNodesDeleted() {
                return 1;
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

    private void reloadValidTransactionLines(List<String[]> lines, IwanMappingStrategy strategy) {
        if (!lines.isEmpty()) {
            Transaction tx = graphDb.beginTx();
            lines.forEach(ct -> importLine(ct, strategy));
            tx.success();
            tx.close();
        }
    }

    private void importLine(String[] line, IwanMappingStrategy strategy) {
        Stopwatch sw = Stopwatch.createStarted();
        //TODO
//        if (line.length != 5) {
//            throw new IllegalArgumentException("Invalid IWAN line for import, expected 5 columns, got " + line.length);
//        }
        Node clientNode = createClient(line, strategy);
        Node areaNode = createArea(line, strategy, clientNode);
        Node siteNode = createSite(line, strategy, clientNode);
        Node cpeNode = createCpe(line, strategy, siteNode);
        Node interfaceNode = createInterface(line, strategy, cpeNode);

        long clientId = Long.parseLong(line[strategy.getColumnIndex("cluster_client_id")]);
        String clientAttribute = "client:" + clientId;
        ImmutableMultimap<String, Node> planets = getPlanetsForClient(clientAttribute);

        planets.get("cluster:client").forEach(n ->
                clientNode.createRelationshipTo(n, LINK_ATTRIBUTE));
        planets.get("cluster:area").forEach(n ->
                areaNode.createRelationshipTo(n, LINK_ATTRIBUTE));
        planets.get("cluster:site").forEach(n ->
                siteNode.createRelationshipTo(n, LINK_ATTRIBUTE));
        planets.get("neType:cpe").forEach(n ->
                cpeNode.createRelationshipTo(n, LINK_ATTRIBUTE));
        planets.get("neType:interface").forEach(n ->
                interfaceNode.createRelationshipTo(n, LINK_ATTRIBUTE));

        areaNode.createRelationshipTo(clientNode, LINK_CONNECT);
        siteNode.createRelationshipTo(clientNode, LINK_CONNECT);
        siteNode.createRelationshipTo(areaNode, LINK_CONNECT);
        cpeNode.createRelationshipTo(siteNode, LINK_CONNECT);
        interfaceNode.createRelationshipTo(cpeNode, LINK_CONNECT);

        LOGGER.info("importLine: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
    }

    private Node createClient(String[] line, IwanMappingStrategy strategy) {
        String clusterClientTag = line[strategy.getColumnIndex("cluster_client_tag")];
        UniqueEntity<Node> clientEntity = networkElementFactory.getOrCreateWithOutcome("tag", clusterClientTag);
        Node clientNode = clientEntity.entity();
        if (clientEntity.wasCreated()) {
            clientNode.addLabel(LABEL_SCOPE);
            clientNode.setProperty(_TYPE, "cluster:client");
            clientNode.setProperty(ID, line[strategy.getColumnIndex("cluster_client_id")]);
        } else {
            clientNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
        }
        clientNode.setProperty(NAME, line[strategy.getColumnIndex("cluster_client_name")]);

        return clientNode;
    }

    private Node createArea(String[] line, IwanMappingStrategy strategy, Node parent) {
        String clusterClientTag = line[strategy.getColumnIndex("cluster_area_tag")];
        UniqueEntity<Node> areaEntity = networkElementFactory.getOrCreateWithOutcome("tag", clusterClientTag);
        Node areaNode = areaEntity.entity();
        if (areaEntity.wasCreated()) {
            areaNode.setProperty(_TYPE, "cluster:area");
            areaNode.setProperty(ID, line[strategy.getColumnIndex("cluster_area_id")]);
        } else {
            areaNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
            areaNode.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("Connect"))
                    .forEach(r -> {
                        Node endNode = r.getEndNode();
                        if (!endNode.equals(parent)) {
                            r.delete();
                        }
                    });
        }
        areaNode.setProperty(NAME, line[strategy.getColumnIndex("cluster_area_name")]);

        return areaNode;
    }

    private Node createSite(String[] line, IwanMappingStrategy strategy, Node parent) {
        String parentType = parent.getProperty("_type").toString();
        String clusterClientTag = line[strategy.getColumnIndex("cluster_site_tag")];
        UniqueEntity<Node> siteEntity = networkElementFactory.getOrCreateWithOutcome("tag", clusterClientTag);
        Node siteNode = siteEntity.entity();
        if (siteEntity.wasCreated()) {
            siteNode.setProperty(_TYPE, "cluster:site");
            siteNode.setProperty(ID, line[strategy.getColumnIndex("cluster_site_id")]);
        } else {
            siteNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
            siteNode.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("Connect"))
                    .forEach(r -> {
                        Node endNode = r.getEndNode();
                        if (endNode.getProperty(_TYPE).equals(parentType) && !endNode.equals(parent)) {
                            r.delete();
                        }
                    });
        }
        siteNode.setProperty(NAME, line[strategy.getColumnIndex("cluster_site_name")]);

        return siteNode;
    }

    private Node createCpe(String[] line, IwanMappingStrategy strategy, Node parent) {
        String clusterClientTag = line[strategy.getColumnIndex("neType_cpe_tag")];
        UniqueEntity<Node> cpeEntity = networkElementFactory.getOrCreateWithOutcome("tag", clusterClientTag);
        Node cpeNode = cpeEntity.entity();
        if (cpeEntity.wasCreated()) {
            cpeNode.setProperty(_TYPE, "neType:cpe");
            cpeNode.setProperty(ID, line[strategy.getColumnIndex("neType_cpe_id")]);
        } else {
            cpeNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
            cpeNode.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("Connect"))
                    .forEach(r -> {
                        Node endNode = r.getEndNode();
                        if (!endNode.equals(parent)) {
                            r.delete();
                        }
                    });
        }
        cpeNode.setProperty(NAME, line[strategy.getColumnIndex("neType_cpe_name")]);
        cpeNode.setProperty("ip", line[strategy.getColumnIndex("neType_cpe_ip")]);

        return cpeNode;
    }

    private Node createInterface(String[] line, IwanMappingStrategy strategy, Node parent) {
        String neTypeInterfaceTag = line[strategy.getColumnIndex("neType_interface_tag")];
        UniqueEntity<Node> interfaceEntity = networkElementFactory.getOrCreateWithOutcome("tag", neTypeInterfaceTag);
        Node interfaceNode = interfaceEntity.entity();
        if (interfaceEntity.wasCreated()) {
            interfaceNode.setProperty(_TYPE, "neType:interface");
            interfaceNode.setProperty(ID, line[strategy.getColumnIndex("neType_interface_id")]);
        } else {
            interfaceNode.setProperty(UPDATED_AT, Instant.now().toEpochMilli());
            interfaceNode.getRelationships(Direction.OUTGOING, DynamicRelationshipType.withName("Connect"))
                    .forEach(r -> {
                        Node endNode = r.getEndNode();
                        if (!endNode.equals(parent)) {
                            r.delete();
                        }
                    });
        }
        interfaceNode.setProperty(NAME, line[strategy.getColumnIndex("neType_interface_name")]);
        interfaceNode.setProperty("ifIndex", Float.parseFloat(line[strategy.getColumnIndex("neType_interface_ifIndex")]));
        interfaceNode.setProperty("ifDescr", line[strategy.getColumnIndex("neType_interface_ifDescr")]);

        return interfaceNode;
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

                planet.getRelationships(Direction.OUTGOING, LINK_ATTRIBUTE).forEach(l -> {
                    Node attribute = l.getEndNode();
                    if (!attribute.hasLabel(LABEL_ATTRIBUTE)) {
                        return;
                    }
                    String type = attribute.getProperty(_TYPE).toString();
                    if (KEY_TYPES.contains(type)) {
                        bldr.put(type + ':' + attribute.getProperty(NAME), planet);
                    }
                });

            });
            planets = bldr.build();
            planetsByClient.put(clientAttribute, planets);
        }

        LOGGER.info("getPlanetsForClient: {}ms", sw.elapsed(TimeUnit.MILLISECONDS));
        return planets;
    }
}
