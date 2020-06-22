package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents the result of a single RelationshipQuery
 */
public class RelationshipQueryResult {

    public final ImmutableMap<String, Object> result;

    public RelationshipQueryResult(ImmutableMap<String, Object> result) {
        this.result = result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationshipQueryResult that = (RelationshipQueryResult) o;
        return Objects.equal(result, that.result);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(result);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("result", result)
                .toString();
    }
}
