package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public final class PartialSchema {
    public final ImmutableMap<String, CounterNode> counters;
    public final MemdexPathNode path;

    public PartialSchema(@JsonProperty("counters") Map<String, CounterNode> counters,
                         @JsonProperty("path") MemdexPathNode path) {
        this.counters = ImmutableMap.copyOf(counters);
        this.path = path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartialSchema that = (PartialSchema) o;

        if (counters != null ? !counters.equals(that.counters) : that.counters != null) return false;
        return path != null ? path.equals(that.path) : that.path == null;
    }

    @Override
    public int hashCode() {
        int result = counters != null ? counters.hashCode() : 0;
        result = 31 * result + (path != null ? path.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("counters", counters)
                .add("path", path)
                .toString();
    }
}
