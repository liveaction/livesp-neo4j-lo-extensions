package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Objects;

public final class ColumnOrder {

    public enum Direction {
        ASC, DESC
    }

    public final Column column;

    public final Direction direction;

    public ColumnOrder(@JsonProperty("column") Column column,
                       @JsonProperty("direction") Direction direction) {
        this.column = column;
        this.direction = direction;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnOrder that = (ColumnOrder) o;
        return Objects.equals(column, that.column) &&
                direction == that.direction;
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, direction);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("column", column)
                .add("direction", direction)
                .toString();
    }

}
