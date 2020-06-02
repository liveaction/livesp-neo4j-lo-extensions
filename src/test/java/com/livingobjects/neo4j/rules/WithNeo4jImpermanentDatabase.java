package com.livingobjects.neo4j.rules;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ResourceInfo;
import org.apache.commons.io.IOUtils;
import org.junit.rules.ExternalResource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@SuppressWarnings("deprecation")
public class WithNeo4jImpermanentDatabase extends ExternalResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(WithNeo4jImpermanentDatabase.class);

    private DatabaseManagementService databaseManagementService;
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

        databaseManagementService = new DatabaseManagementServiceBuilder(Files.createTempDirectory("neo4j-test-db").toFile())
                .build();
        db = databaseManagementService.database(DEFAULT_DATABASE_NAME);

        LOGGER.debug("Graph database started.");
        initializeDatabase();
    }

    private void initializeDatabase() throws IOException {
        if (cypherStream.isPresent()) {
            cyphers.add(loadCqlFromInputStream(cypherStream.get()));
        }
        long start = System.currentTimeMillis();
        try (Transaction tx = db.beginTx()) {
            for (String q : cyphers) {
                String[] distinctQueries = q.split(";");
                for (String query : distinctQueries) {
                    if (!query.trim().isEmpty()) {
                        try {
                            tx.execute(query);
                        } catch (Exception e) {
                            LOGGER.error("{}: {}\n{}\n", e.getClass(), e.getLocalizedMessage(), query);
                            throw e;
                        }
                    }
                }
            }
            tx.commit();
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
        databaseManagementService.shutdown();
        cypherStream.ifPresent(cs -> {
            try {
                cs.close();
            } catch (IOException ignore) {
            }
        });
        super.after();
    }

    public static String[] éosgiSystemPackages() {
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
