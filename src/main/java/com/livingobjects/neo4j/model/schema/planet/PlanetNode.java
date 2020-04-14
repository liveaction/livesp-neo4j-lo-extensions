package com.livingobjects.neo4j.model.schema.planet;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public final class PlanetNode {

    public final String type;

    public final String name;

    public final ImmutableSet<String> attributes;

    public PlanetNode(@JsonProperty("type") String type,
                      @JsonProperty("name") String name,
                      @JsonProperty("attributes") List<String> attributes) {
        this.type = type;
        this.name = name;
        this.attributes = ImmutableSet.copyOf(attributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PlanetNode that = (PlanetNode) o;

        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("name", name)
                .add("attributes", attributes)
                .toString();
    }
}
