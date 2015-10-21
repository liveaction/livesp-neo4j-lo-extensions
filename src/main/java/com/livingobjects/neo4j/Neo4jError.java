package com.livingobjects.neo4j;

import org.codehaus.jackson.annotate.JsonProperty;

public final class Neo4jError {

    public final String code;

    public final String message;

    public Neo4jError(@JsonProperty("code") String code, @JsonProperty("message") String message) {
        this.code = code;
        this.message = message;
    }
}
