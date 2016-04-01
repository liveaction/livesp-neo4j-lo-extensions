package com.livingobjects.neo4j.model;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public final class Neo4jQuery {

    public final String statement;

    public final Map<String, Object> parameters;

    public Neo4jQuery(@JsonProperty("statement") String statement, @JsonProperty("parameters") Map<String, Object> parameters) {
        this.statement = statement;
        this.parameters = parameters;
    }

}
