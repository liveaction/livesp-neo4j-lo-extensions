package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class Column {

    public final String keyAttribute;

    public final String property;

    public Column(@JsonProperty("keyAttribute") String keyAttribute,
                  @JsonProperty("property") String property) {
        this.keyAttribute = keyAttribute;
        this.property = property;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Objects.equals(keyAttribute, column.keyAttribute) &&
                Objects.equals(property, column.property);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyAttribute, property);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("keyAttribute", keyAttribute)
                .add("property", property)
                .toString();
    }
}
