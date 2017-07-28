package com.livingobjects.neo4j.migrations;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants._SCOPE;

public final class Migration_2_00_0 {
    private static final Logger LOGGER = LoggerFactory.getLogger(Migration_2_00_0.class);
    private static final int MAX_TX_STATEMENT = 10;
    private static final Executor executor = Executors.newSingleThreadExecutor();

    private final Informations infoLog = new Informations();
    private final GraphDatabaseService graphDb;

    private Migration_2_00_0(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public static Migration_2_00_0 prepareMigration(GraphDatabaseService graphDb) {
        return new Migration_2_00_0(graphDb);
    }

    public static void cleanUpgrade(final GraphDatabaseService graphDb) {
        Migration_2_00_0 migration_2_00_0 = new Migration_2_00_0(graphDb);
        migration_2_00_0.cleanUpgrade();
    }

    public void cleanUpgrade() {
        Stopwatch timer = Stopwatch.createStarted();
        infoLog.log("Migrate to 2.00.0 ...");

        clearOldTemplates();
        clearApplicationRelations();
        clearUselessPlanet();
        relinkApplication();

        LOGGER.info("... migration to 2.00.0 finished in {}mn", timer.elapsed(TimeUnit.MINUTES));
    }

    public Informations getProgression() {
        return infoLog;
    }

    private void clearOldTemplates() {
        try (Transaction tx = graphDb.beginTx()) {
            Iterator<Node> templateIt = graphDb.findNodes(DynamicLabel.label("Template"));
            while (templateIt.hasNext()) {
                deleteNode(templateIt.next());
            }
            tx.success();
        }
        infoLog.log("... old template cleared !");
    }

    private void clearApplicationRelations() {
        Stopwatch timer = Stopwatch.createStarted();
        Iterable<Node> nodes;
        try (Transaction tx = graphDb.beginTx()) {
            nodes = Iterables.asResourceIterable(graphDb.findNodes(Labels.ELEMENT, IwanModelConstants._TYPE, "neType:application"));
            tx.success();
        }

        Transaction tx = graphDb.beginTx();
        try {
            int count = 0;
            for (Node node : nodes) {
                node.getRelationships(Direction.OUTGOING, RelationshipTypes.ATTRIBUTE).forEach(Relationship::delete);
                if (++count >= MAX_TX_STATEMENT) {
                    tx = commit(graphDb, tx);
                    count = 0;
                }
            }
            LOGGER.info("... all application relations cleared in {} mn !", timer.elapsed(TimeUnit.MINUTES));
        } catch (Exception e) {
            LOGGER.error("{}: {}", e.getClass(), e.getLocalizedMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("STACKTRACE", e);
            }

        } finally {
            tx.success();
            tx.close();
        }
    }

    private void clearUselessPlanet() {
        ImmutableList<Pattern> removes = ImmutableList.of(
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/path/site/right"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/path/dscp"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/path/site/left"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/snmp/dscp/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/media-perf/application/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/media-perf/dscp/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/media-perf/dscp/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/media-perf/viewpoint/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/wan-optimization/dscp/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/wan-optimization/cpe/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/wan-optimization/application/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/cs-perf/dscp/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/cs-perf/application/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/cs-perf/cpe/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/traffic/internalip/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/netflow/traffic/dscp/cisco"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/application/category"),
                Pattern.compile("iwan/[a-zA-Z0-9]*/application/group")
        );

        Iterable<Node> nodes;
        try (Transaction tx = graphDb.beginTx()) {
            nodes = Iterables.asResourceIterable(graphDb.findNodes(Labels.PLANET));
            tx.success();
        }

        Transaction tx = graphDb.beginTx();
        try {
            List<Node> toDelete = Lists.newArrayListWithCapacity(MAX_TX_STATEMENT * 100);
            for (Node planet : nodes) {
                String name = planet.getProperty(NAME).toString();
                for (Pattern remove : removes) {
                    if (remove.matcher(name).matches()) {
                        toDelete.add(planet);
                        if (toDelete.size() >= MAX_TX_STATEMENT * 100) {
                            applyDeletion(graphDb, toDelete);
                        }
                        break;
                    }
                }
            }
            applyDeletion(graphDb, toDelete);
        } catch (Exception e) {
            LOGGER.error("{}: {}", e.getClass(), e.getLocalizedMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("STACKTRACE", e);
            }
            return;

        } finally {
            tx.success();
            tx.close();
        }


        ImmutableList<Entry<Pattern, String>> renames = ImmutableList.of(
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/snmp/wanLink/cisco"), "iwan/{:scopeId}/wanLink/cisco"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/netflow/traffic/viewpoint/cisco"), "iwan/{:scopeId}/viewpoint/cisco"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/netflow/path/network"), "iwan/{:scopeId}/network"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/netflow/path/mastercontroller"), "iwan/{:scopeId}/mastercontroller"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/snmp/ipslaProbe/cisco"), "iwan/{:scopeId}/ipslaProbe/cisco"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/snmp/cpe/cisco"), "iwan/{:scopeId}/cpe/cisco"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/snmp/cos"), "iwan/{:scopeId}/cos"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/netflow/path/borderrouter"), "iwan/{:scopeId}/borderrouter"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/area"), "iwan/{:scopeId}/area"),
                Maps.immutableEntry(Pattern.compile("iwan/(?<cid>[a-zA-Z0-9]*)/netflow/traffic/application/cisco"), "iwan/{:scopeId}/application/cisco")
        );

        try {
            tx = graphDb.beginTx();
            Iterator<Node> planetNodes = graphDb.findNodes(Labels.PLANET);
            while (planetNodes.hasNext()) {
                Node planet = planetNodes.next();
                String name = planet.getProperty(NAME).toString();
                for (Entry<Pattern, String> rename : renames) {
                    planet.getRelationships().forEach(rl -> {
                        if (!rl.getStartNode().hasLabel(Labels.ELEMENT)) {
                            rl.delete();
                        }
                    });
                    Matcher m = rename.getKey().matcher(name);
                    if (m.matches()) {
                        String scopeId = m.group("cid");
                        if (!scopeId.isEmpty()) {
                            planet.setProperty(NAME, rename.getValue().replace("{:scopeId}", scopeId));
                        } else {
                            LOGGER.error("Unable to find scopeId for {}", name);
                        }
                        break;
                    }
                }
            }
            LOGGER.info("... all useless Planet cleared !");

        } catch (Exception e) {
            LOGGER.error("{}: {}", e.getClass(), e.getLocalizedMessage());
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("STACKTRACE", e);
            }
        } finally {
            tx.success();
            tx.close();
        }
    }

    private void applyDeletion(GraphDatabaseService graphDb, List<Node> toDelete) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        executor.execute(() -> {
            try (Transaction ntx = graphDb.beginTx()) {
                toDelete.forEach(Migration_2_00_0::deleteNode);
                ntx.success();
            }
            toDelete.clear();
            latch.countDown();
        });
        latch.await();
    }

    private void relinkApplication() {
        try (Transaction tx = graphDb.beginTx()) {
            Node globalApp = graphDb.findNode(Labels.PLANET, NAME, "iwan/global/application/cisco");
            Node spApp = graphDb.findNode(Labels.PLANET, NAME, "iwan/sp/application/cisco");
            graphDb.findNodes(Labels.ELEMENT, IwanModelConstants._TYPE, "neType:application").forEachRemaining(appNode -> {
                String scope = appNode.getProperty(_SCOPE, "class=scope,scope=global").toString();
                switch (scope) {
                    case "class=scope,scope=global":
                        appNode.createRelationshipTo(globalApp, RelationshipTypes.ATTRIBUTE);
                        break;

                    case "class=scope,scope=sp":
                        appNode.createRelationshipTo(spApp, RelationshipTypes.ATTRIBUTE);
                        break;

                    default:
                        String client = Splitter.on(',').withKeyValueSeparator('=').split(scope)
                                .get("client");
                        Node clientApp = graphDb.findNode(Labels.PLANET, NAME, "iwan/" + client + "/application/cisco");
                        appNode.createRelationshipTo(clientApp, RelationshipTypes.ATTRIBUTE);
                        break;
                }
            });
            tx.success();
        }
        LOGGER.info("... {} linked with Planet {} !", "neType:application", "iwan/global/application/cisco");

        ImmutableMap.of(
                "neType:dscp", "iwan/global/dscp/cisco",
                "cluster:application/group", "iwan/global/application/group/cisco",
                "cluster:application/category", "iwan/global/application/category/cisco"
        ).forEach((key, value) -> {
            try (Transaction tx = graphDb.beginTx()) {
                Node globalDscp = graphDb.findNode(Labels.PLANET, NAME, value);
                globalDscp.getRelationships(RelationshipTypes.ATTRIBUTE).forEach(rl -> {
                    if (rl.getStartNode().hasLabel(Labels.ELEMENT)) {
                        rl.delete();
                    }
                });
                graphDb.findNodes(Labels.ELEMENT, IwanModelConstants._TYPE, key).forEachRemaining(dscpNode ->
                        dscpNode.createRelationshipTo(globalDscp, RelationshipTypes.ATTRIBUTE));
                tx.success();
            }
            LOGGER.info("... {} linked with Planet {} !", key, value);
        });
    }

    private static Transaction commit(final GraphDatabaseService graphDb, Transaction tx) {
        tx.success();
        tx.close();
        return graphDb.beginTx();
    }

    private static void deleteNode(Node node) {
        node.getRelationships().forEach(Relationship::delete);
        node.delete();
    }
}
