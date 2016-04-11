package com.livingobjects.neo4j.iwan.model;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.index.UniqueFactory.UniqueNodeFactory;

import java.time.Instant;
import java.util.Map;

public final class NetworkElementFactory extends UniqueNodeFactory {
    private NetworkElementFactory(GraphDatabaseService graphdb, String index) {
        super(graphdb, index);
    }

    @Override
    protected void initialize(Node created, Map<String, Object> properties) {
        created.addLabel(DynamicLabel.label("NetworkElement"));
        created.addLabel(DynamicLabel.label("Element"));
        created.setProperty("tag", properties.get("tag"));
        created.setProperty("createdAt", Instant.now().toEpochMilli());
    }

    public static UniqueNodeFactory build(GraphDatabaseService graphdb) {
        return new NetworkElementFactory(graphdb, "NetworkElement");
    }
}
