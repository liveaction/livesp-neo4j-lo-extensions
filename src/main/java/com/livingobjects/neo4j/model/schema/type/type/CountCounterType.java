package com.livingobjects.neo4j.model.schema.type.type;

import com.google.common.base.MoreObjects;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class CountCounterType extends CounterType {

    public final String countType;

    public CountCounterType(@JsonProperty("counterType") String countType) {
        this.countType = countType;
    }

    @Override
    public <R> R visit(Visitor<R> visitor) {
        return visitor.count(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CountCounterType that = (CountCounterType) o;
        return Objects.equals(countType, that.countType);
    }

    @Override
    public int hashCode() {

        return Objects.hash(countType);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("counterType", countType)
                .toString();
    }
}
