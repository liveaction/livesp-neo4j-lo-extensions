package com.livingobjects.neo4j;

import org.neo4j.graphdb.*;
import org.neo4j.tooling.GlobalGraphOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

@Path("/warmup")
public class WarmupExtension {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarmupExtension.class);
    private static final int MB = 1024 * 1024;

    private final GraphDatabaseService graphDb;

    public WarmupExtension(@Context GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    /**
     * Perform the warmup of all nodes with all relationships and properties.
     */
    @GET()
    public Response warmUp() {
        Runtime runtime = Runtime.getRuntime();
        LOGGER.info("Warming-up graph database... (memory usage : {}/{} Mb)", getUsedMemory(runtime), getMaxMemory(runtime));
        try (Transaction transaction = graphDb.beginTx()) {
            ResourceIterable<Node> allNodes = GlobalGraphOperations.at(graphDb).getAllNodes();
            for (Node node : allNodes) {
                accessEverything(node);
            }
            transaction.success();
        }
        LOGGER.info("Graph database warm-up finished. (memory usage : {}/{} Mb)", getUsedMemory(runtime), getMaxMemory(runtime));
        return Response.ok().build();
    }

    /**
     * Access the given node's properties, labels, relationships, and their properties.
     *
     * @param node to access.
     */
    private void accessEverything(Node node) {
        for (Relationship r : node.getRelationships(Direction.OUTGOING)) {
            r.getOtherNode(node);
            r.getType();
            for (String key : r.getPropertyKeys()) {
                r.getProperty(key);
            }
        }
        for (String key : node.getPropertyKeys()) {
            node.getProperty(key);
        }
        node.getLabels();
    }

    private long getMaxMemory(Runtime runtime) {
        return (runtime.maxMemory()) / MB;
    }

    private long getUsedMemory(Runtime runtime) {
        return (runtime.totalMemory() - runtime.freeMemory()) / MB;
    }

}
