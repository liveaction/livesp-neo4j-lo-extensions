package com.livingobjects.neo4j.model.result;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public final class Neo4jLoadResult {

    public final int imported;

    public final Map<Integer, String> errorLines;

    public Neo4jLoadResult(@JsonProperty("imported") int imported, @JsonProperty("errorLines") Map<Integer, String> errorLines) {
        this.imported = imported;
        this.errorLines = errorLines;
    }
}
