package com.livingobjects.neo4j.iwan.model.schema.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;

public class Relationship {

    public final Node from;

    public final Node to;

    public final String type;

    public final ImmutableSet<Property> properties;

    public Relationship(Node from, Node to, String type, ImmutableSet<Property> properties) {
        this.from = from;
        this.to = to;
        this.type = type;
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Relationship that = (Relationship) o;
        return Objects.equals(from, that.from) &&
                Objects.equals(to, that.to) &&
                Objects.equals(type, that.type) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to, type, properties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("from", from)
                .add("to", to)
                .add("type", type)
                .add("properties", properties)
                .toString();
    }
}
