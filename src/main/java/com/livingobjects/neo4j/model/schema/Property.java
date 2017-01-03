package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.livingobjects.neo4j.model.PropertyType;

import java.util.Objects;

public final class Property {

    public final String name;

    public final String value;

    public final PropertyType type;

    public final boolean isArray;

    public Property(String name, String value, PropertyType type, boolean isArray) {
        this.name = name;
        this.value = value;
        this.type = type;
        this.isArray = isArray;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Property property = (Property) o;
        return isArray == property.isArray &&
                Objects.equals(name, property.name) &&
                Objects.equals(value, property.value) &&
                type == property.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, value, type, isArray);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("value", value)
                .add("type", type)
                .add("isArray", isArray)
                .toString();
    }
}
