package com.livingobjects.neo4j.helper;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CacheNodeLoader {

    private static final Logger LOGGER = LoggerFactory.getLogger(CacheNodeLoader.class);

    public final LoadingCache<String, ImmutableSet<Node>> cache = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, ImmutableSet<Node>>() {
                @Override
                public ImmutableSet<Node> load(String cypher) throws Exception {
                    long time = System.currentTimeMillis();
                    try (Result execute = graphDB.execute(cypher)) {
                        ImmutableSet.Builder<Node> nodesBuilder = ImmutableSet.builder();
                        while (execute.hasNext()) {
                            for (Object result : execute.next().values()) {
                                if (result != null) {
                                    if (result instanceof Node) {
                                        Node node = (Node) result;
                                        nodesBuilder.add(node);
                                    } else {
                                        throw new IllegalStateException("Unable to create relationship with cypher statement '" + cypher + "'. It must returns a node instance.");
                                    }
                                }
                            }
                        }

                        ImmutableSet<Node> nodes = nodesBuilder.build();
                        LOGGER.trace("{} nodes loaded from cypher query in {} ms.", nodes.size(), (System.currentTimeMillis() - time));
                        return nodes;
                    } catch (QueryExecutionException e) {
                        throw new IllegalStateException("Unable to execute cypher query to make relationships", e);
                    }
                }
            });

    private final GraphDatabaseService graphDB;

    public CacheNodeLoader(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
    }

    public final ImmutableSet<Node> load(String cypher) {
        try {
            return cache.get(cypher);
        } catch (Exception e) {
            throw new IllegalStateException("Unable to load cache from cypher query", e);
        }
    }

    public void clear() {
        cache.invalidateAll();
    }
}
