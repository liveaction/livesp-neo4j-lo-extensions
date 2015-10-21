package com.livingobjects.neo4j;

import org.codehaus.jackson.annotate.JsonProperty;
import org.neo4j.graphdb.QueryStatistics;

public final class Neo4jResult {

    public final QueryStatistics stats;

    public Neo4jResult(@JsonProperty("stats") QueryStatistics stats) {
        this.stats = stats;
    }
}
