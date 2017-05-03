package com.livingobjects.neo4j.model.result;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;
import java.util.Set;

public final class Neo4jLoadResult {

    public final Map<TypedScope, Set<String>> importedElementsByScope;

    public final Map<Integer, String> errorLines;

    public Neo4jLoadResult(@JsonProperty("importedElementsByScope")
                                   Map<TypedScope, Set<String>> importedElementsByScope,
                           @JsonProperty("errorLines")
                                   Map<Integer, String> errorLines) {
        this.importedElementsByScope = importedElementsByScope;
        this.errorLines = errorLines;
    }

}
