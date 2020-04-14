package com.livingobjects.neo4j.model.result;

import com.fasterxml.jackson.annotation.JsonProperty;

public final class Neo4jErrorResult {

    public final String code;

    public final String message;

    public Neo4jErrorResult(@JsonProperty("code") String code, @JsonProperty("message") String message) {
        this.code = code;
        this.message = message;
    }
}
