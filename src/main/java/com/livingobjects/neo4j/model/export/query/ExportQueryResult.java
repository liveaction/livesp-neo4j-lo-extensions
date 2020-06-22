package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Represents the result of a single ExportQuery
 */
public class ExportQueryResult {

    public final ImmutableMap<String, Map<String, Object>> result;

    public ExportQueryResult(ImmutableMap<String, Map<String, Object>> result) {
        this.result = result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExportQueryResult that = (ExportQueryResult) o;
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
