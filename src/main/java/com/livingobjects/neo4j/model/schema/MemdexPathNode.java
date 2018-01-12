package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public final class MemdexPathNode {
    public final ImmutableSet<String> planets;
    public final String segment;
    public final ImmutableSet<String> attributes;
    public final ImmutableSet<String> counters;
    public final ImmutableSet<MemdexPathNode> children;

    public MemdexPathNode(@JsonProperty("planets") List<String> planets,
                          @JsonProperty("segment") String segment,
                          @JsonProperty("attributes") List<String> attributes,
                          @JsonProperty("counters") List<String> counters,
                          @JsonProperty("children") List<MemdexPathNode> children) {
        this.planets = ImmutableSet.copyOf(planets);
        this.segment = segment;
        this.attributes = ImmutableSet.copyOf(attributes);
        this.counters = ImmutableSet.copyOf(counters);
        this.children = ImmutableSet.copyOf(children);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MemdexPathNode that = (MemdexPathNode) o;

        if (planets != null ? !planets.equals(that.planets) : that.planets != null) return false;
        if (segment != null ? !segment.equals(that.segment) : that.segment != null) return false;
        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
        if (counters != null ? !counters.equals(that.counters) : that.counters != null) return false;
        return children != null ? children.equals(that.children) : that.children == null;
    }

    @Override
    public int hashCode() {
        int result = planets != null ? planets.hashCode() : 0;
        result = 31 * result + (segment != null ? segment.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (counters != null ? counters.hashCode() : 0);
        result = 31 * result + (children != null ? children.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("planets", planets)
                .add("segment", segment)
                .add("attributes", attributes)
                .add("counters", counters)
                .add("children", children)
                .toString();
    }
}
