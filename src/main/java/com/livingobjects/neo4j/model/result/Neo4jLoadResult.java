package com.livingobjects.neo4j.model.result;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class Neo4jLoadResult {

    public final Map<TypedScope, Set<String>> importedElementsByScope;

    public final int imported;

    public final Map<Integer, String> errorLines;

    public Neo4jLoadResult(@JsonProperty("imported") int imported,
                           @JsonProperty("errorLines") Map<Integer, String> errorLines,
                           @JsonProperty("importedElementsByScope") Map<TypedScope, Set<String>> importedElementsByScope) {
        this.importedElementsByScope = importedElementsByScope;
        this.imported = imported;
        this.errorLines = errorLines;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Neo4jLoadResult that = (Neo4jLoadResult) o;
        return imported == that.imported &&
                Objects.equals(importedElementsByScope, that.importedElementsByScope) &&
                Objects.equals(errorLines, that.errorLines);
    }

    @Override
    public int hashCode() {
        return Objects.hash(importedElementsByScope, imported, errorLines);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("importedElementsByScope", importedElementsByScope)
                .add("imported", imported)
                .add("errorLines", errorLines)
                .toString();
    }

}
