package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

public final class PartialSchema {
    public final ImmutableMap<String, CounterNode> counters;
    public final MemdexPathNode memdexPath;
    public final ImmutableList<String> attributes;
    public final String name;

    public PartialSchema(@JsonProperty("counters") Map<String, CounterNode> counters,
                         @JsonProperty("memdexPath") MemdexPathNode memdexPath,
                         @JsonProperty("attributes") List<String> attributes,
                         @JsonProperty("name") String name) {
        this.counters = ImmutableMap.copyOf(counters);
        this.memdexPath = memdexPath;
        this.attributes = ImmutableList.copyOf(attributes);
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartialSchema that = (PartialSchema) o;

        if (counters != null ? !counters.equals(that.counters) : that.counters != null) return false;
        return memdexPath != null ? memdexPath.equals(that.memdexPath) : that.memdexPath == null;
    }

    @Override
    public int hashCode() {
        int result = counters != null ? counters.hashCode() : 0;
        result = 31 * result + (memdexPath != null ? memdexPath.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("counters", counters)
                .add("memdexPath", memdexPath)
                .toString();
    }
}
