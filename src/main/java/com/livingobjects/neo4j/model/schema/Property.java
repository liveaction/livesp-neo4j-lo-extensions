package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;

import java.util.Objects;

public final class Property {

    public enum Type {STRING, NUMBER, BOOLEAN}

    public final String name;

    public final String value;

    public final Type type;

    public Property(String name, String value, Type type) {
        this.name = name;
        this.value = value;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Property property = (Property) o;
        return Objects.equals(name, property.name) &&
                Objects.equals(value, property.value) &&
                Objects.equals(type, property.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, type);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .add("type", type)
                .toString();
    }
}
