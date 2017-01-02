package com.livingobjects.neo4j.iwan.model.schema.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;

public final class Relationship {

    public final String node;

    public final ImmutableSet<Property> properties;

    public Relationship(String node, ImmutableSet<Property> properties) {
        this.node = node;
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Relationship that = (Relationship) o;
        return Objects.equals(node, that.node) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, properties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("node", node)
                .add("properties", properties)
                .toString();
    }
}
