package com.livingobjects.neo4j.rules;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;
import org.apache.commons.io.IOUtils;
import org.junit.rules.ExternalResource;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.NullLog;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;
import java.util.logging.Level;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;

@SuppressWarnings("deprecation")
public class WithNeo4jImpermanentDatabase extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(WithNeo4jImpermanentDatabase.class);
    private static final int RANDOM_PORTS_LOWER_BOUND = 9200;
    private static final int RANDOM_PORTS_COUNT = 50000;
    private static final String NEO4J_EMBEDDED_HOST = "127.0.0.1";

    private GraphDatabaseService db;

    //private WrappingNeoServerBootstrapper neoServerBootstrapper;
    private List<String> cyphers = new ArrayList<>();

    private Optional<InputStream> cypherStream = Optional.empty();

    private static final Map<String, List<ResourceInfo>> resourcesCache = Maps.newConcurrentMap();

    public WithNeo4jImpermanentDatabase() {
    }

    public final WithNeo4jImpermanentDatabase withDatapacks(String name) {
        List<ResourceInfo> cqls = resourcesCache.computeIfAbsent(name, k -> {
            try {
                ImmutableSet<ResourceInfo> resources = ClassPath.from(ClassLoader.getSystemClassLoader()).getResources();
                return resources.stream().filter(resourceInfo ->
                        resourceInfo.getResourceName().matches("datapacks/" + name + ".*\\.cql"))
                        .sorted(Comparator.comparing(ResourceInfo::getResourceName))
                        .collect(Collectors.toList());
            } catch (IOException e) {
                throw new NoSuchElementException(name);
            }
        });
        LOGGER.warn("Load datapack {} with {} file(s).", name, cqls.size());
        cqls.forEach(ri -> {
            try (InputStream in = ri.url().openStream()) {
                readInputStreamAsQuery(in);
            } catch (IOException e) {
                fail(e.getLocalizedMessage());
            }
        });
        return this;
    }

    public final WithNeo4jImpermanentDatabase withFixture(Path... cypherPath) {
        URL resource = getClass().getResource("/");
        Path root = Paths.get(resource.getPath());
        for (Path cp : cypherPath) {
            Path path = Paths.get(resource.getPath(), cp.toString());
            if (Files.isDirectory(path)) {
                try {
                    Files.list(path)
                            .filter(p -> p.toString().endsWith(".cql"))
                            .sorted()
                            .forEach(p -> readPathAsQuery(Paths.get(File.separator, root.relativize(p).toString())));
                } catch (IOException e) {
                    fail(e.getLocalizedMessage());
                }
            } else {
                readPathAsQuery(cp);
            }
        }
        return this;
    }

    private void readPathAsQuery(Path path) {
        try (InputStream in = getClass().getResourceAsStream(path.toString())) {
            if (in == null) {
                throw new NoSuchElementException(path.toString());
            }
            readInputStreamAsQuery(in);
        } catch (Exception e) {
            fail(e.getLocalizedMessage());
        }
    }

    private void readInputStreamAsQuery(InputStream in) {
        try {
            String cypher = IOUtils.readLines(in).stream()
                    .filter(l -> !l.startsWith("//"))
                    .collect(Collectors.joining(" "));
            this.cyphers.add(cypher);
        } catch (IOException e) {
            fail(e.getLocalizedMessage());
        }
    }

    public final WithNeo4jImpermanentDatabase withFixture(String... cypherQuery) {
        this.cyphers.addAll(Arrays.asList(cypherQuery));
        return this;
    }

    public final WithNeo4jImpermanentDatabase withFixture(InputStream cypherStream) {
        this.cypherStream = Optional.of(cypherStream);
        return this;
    }

    public GraphDatabaseService getGraphDatabaseService() {
        return db;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void before() throws Throwable {
        super.before();

//        SLF4JBridgeHandler.removeHandlersForRootLogger();
//        SLF4JBridgeHandler.install();

        Map<String, String> config = new HashMap<>();
        //config.put(ServerSettings.awebserver_address.name(), NEO4J_EMBEDDED_HOST);
        //config.put(ServerSettings.webserver_port.name(), Integer.toString(port));
        //config.put(ServerSettings.auth_enabled.name(), Boolean.toString(false));
        //config.put(ServerSettings.http_logging_enabled.name(), Boolean.toString(false));

        db = new GraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder(Files.createTempDirectory("neo4j-test-db").toFile())
                .setConfig(config).newGraphDatabase();

        /*db = new TestGraphDatabaseFactory().newImpermanentDatabase();

        boolean available = db.isAvailable(5000);
        assert available;

        int start = -1;
        Random random = new Random();
        while (start != 0) {
            int port = RANDOM_PORTS_LOWER_BOUND + random.nextInt(RANDOM_PORTS_COUNT);
            try {
                Map<String, String> config = new HashMap<>();
                config.put(ServerSettings.webserver_address.name(), NEO4J_EMBEDDED_HOST);
                config.put(ServerSettings.webserver_port.name(), Integer.toString(port));
                config.put(ServerSettings.auth_enabled.name(), Boolean.toString(false));
                config.put(ServerSettings.http_logging_enabled.name(), Boolean.toString(false));
                ConfigWrappingConfigurator confBuilder = new ConfigWrappingConfigurator(new Config(config));

                neoServerBootstrapper = new WrappingNeoServerBootstrapper((GraphDatabaseAPI) db, confBuilder);
                start = neoServerBootstrapper.start(null);
                new GraphDatabaseFactory()
                        .newEmbeddedDatabaseBuilder(testDirectory.graphDbDir())
                java.util.logging.Logger l0 = java.util.logging.Logger.getLogger("");
                l0.removeHandler(l0.getHandlers()[0]);
                l0.setLevel(Level.OFF);
            } catch (Exception e) {
                start = -1;
            }
        }

        // Pas bien
        Field log = Bootstrapper.class.getDeclaredField("log");
        log.setAccessible(true);
        log.set(neoServerBootstrapper, NullLog.getInstance());*/

        LOGGER.debug("Graph database started.");
        initializeDatabase();
    }

    private void initializeDatabase() throws IOException {
        if (cypherStream.isPresent()) {
            cyphers.add(loadCqlFromInputStream(cypherStream.get()));
        }

        long start = System.currentTimeMillis();
        for (String q : cyphers) {
            String[] distinctQueries = q.split(";");
            for (String query : distinctQueries) {
                if (!query.trim().isEmpty()) {
                    try {
                        db.execute(query);
                    } catch (Exception e) {
                        LOGGER.error("{}: {}\n{}\n", e.getClass(), e.getLocalizedMessage(), query);
                        throw e;
                    }
                }
            }
        }
        LOGGER.debug("Graph database initialized with fixtures in {}s.", (System.currentTimeMillis() - start) / 1000);
    }

    private String loadCqlFromInputStream(InputStream in) throws IOException {
        return IOUtils.readLines(in).stream()
                .filter(l -> !l.startsWith("//"))
                .collect(Collectors.joining(" "));
    }

    @Override
    public void after() {
        //neoServerBootstrapper.stop();
        db.shutdown();
        cypherStream.ifPresent(cs -> {
            try {
                cs.close();
            } catch (IOException ignore) {
            }
        });
        super.after();
    }

    public static String[] osgiSystemPackages() {
        return new String[]{
                "org.neo4j.harness",
                "org.neo4j.harness.internal",
                "org.neo4j.graphdb",
                "org.neo4j.graphdb.config",
                "org.neo4j.kernel",
                "org.neo4j.kernel.configuration",
                "org.neo4j.server",
                "org.neo4j.server.configuration",
                "org.neo4j.test",
                "org.neo4j.helpers",
                "org.neo4j.logging"
        };
    }
}
