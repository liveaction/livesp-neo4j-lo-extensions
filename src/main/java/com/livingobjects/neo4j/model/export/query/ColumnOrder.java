package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Objects;

public final class ColumnOrder {

    public enum Direction {
        ASC, DESC
    }

    public final String keyAttribute;

    public final String property;

    public final Direction direction;

    public ColumnOrder(@JsonProperty("keyAttribute") String keyAttribute,
                       @JsonProperty("property") String property,
                       @JsonProperty("direction") Direction direction) {
        this.keyAttribute = keyAttribute;
        this.property = property;
        this.direction = direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnOrder that = (ColumnOrder) o;
        return Objects.equals(keyAttribute, that.keyAttribute) &&
                Objects.equals(property, that.property) &&
                direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyAttribute, property, direction);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("keyAttribute", keyAttribute)
                .add("property", property)
                .add("direction", direction)
                .toString();
    }

}
