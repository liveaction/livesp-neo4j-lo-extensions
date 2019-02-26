package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Objects;

public final class Pagination {

    public final int offset;

    public final int limit;

    public Pagination(@JsonProperty("offset") int offset,
                      @JsonProperty("limit") int limit) {
        this.offset = offset;
        this.limit = limit;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pagination that = (Pagination) o;
        return offset == that.offset &&
                limit == that.limit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, limit);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("offset", offset)
                .add("limit", limit)
                .toString();
    }

}
