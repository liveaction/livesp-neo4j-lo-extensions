package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class ExportQuery {

    public final ImmutableSet<String> requiredAttributes;
    public final ImmutableSet<String> parentAttributes;
    public final ImmutableMap<String, Set<String>> columns;
    public final ImmutableMap<String, Map<String, Object>> filter;
    public final boolean includeTag;
    public final ImmutableList<ColumnOrder> sort;
    public final Optional<Pagination> pagination;

    public ExportQuery(@JsonProperty("requiredAttributes") List<String> requiredAttributes,
                       @JsonProperty("parentAttributes") List<String> parentAttributes,
                       @JsonProperty("columns") Map<String, Set<String>> columns,
                       @JsonProperty("filter") Map<String, Map<String, Object>> filter,
                       @JsonProperty("includeTag") boolean includeTag,
                       @JsonProperty("sort") List<ColumnOrder> sort,
                       @JsonProperty("pagination") @Nullable Pagination pagination) {
        this.requiredAttributes = ImmutableSet.copyOf(requiredAttributes);
        this.parentAttributes = ImmutableSet.copyOf(parentAttributes);
        this.columns = ImmutableMap.copyOf(columns);
        this.filter = ImmutableMap.copyOf(filter);
        this.includeTag = includeTag;
        this.sort = ImmutableList.copyOf(sort);
        this.pagination = Optional.ofNullable(pagination);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExportQuery that = (ExportQuery) o;
        return includeTag == that.includeTag &&
                Objects.equals(requiredAttributes, that.requiredAttributes) &&
                Objects.equals(parentAttributes, that.parentAttributes) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(sort, that.sort) &&
                Objects.equals(pagination, that.pagination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requiredAttributes, parentAttributes, columns, filter, includeTag, sort, pagination);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requiredAttributes", requiredAttributes)
                .add("parentAttributes", parentAttributes)
                .add("columns", columns)
                .add("filter", filter)
                .add("includeTag", includeTag)
                .add("sort", sort)
                .add("pagination", pagination)
                .toString();
    }

}
