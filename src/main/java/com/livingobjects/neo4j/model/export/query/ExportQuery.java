package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.export.query.filter.Filter;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class ExportQuery {
    // attributes that will be exported (uses OR)
    public final ImmutableSet<String> requiredAttributes;
    // Parents that will be returned with the attributes (if empty, everything will be returned)
    public final ImmutableSet<String> parentAttributes;
    // columns required in output for required attribute and parent attributes (by default, all columns are included)
    public final ImmutableMap<String, Set<String>> columns;
    // Filter on the attribute (required, parent, or not requested) columns (by default, all filters are linked by AND operator)
    public final Filter<Column> filter;
    // true if you need metadata: tag, createdAt, createdBy, updatedAt, updatedBy
    public final boolean includeMetadata;
    // sort the elements
    public final ImmutableList<ColumnOrder> sort;
    // pagination (/!\: not the same as the Pagination in longback-commons)
    public final Optional<Pagination> pagination;
    // scopes of the query (values can be <client_id>, global or sp). Allows filtering by planets
    public final ImmutableSet<String> scopes;
    // true if you want to export one line for each parent when an element has several parents of the same type (ex: a Site with several Area)
    public final boolean parentsCardinality;

    public ExportQuery(@JsonProperty("requiredAttributes") List<String> requiredAttributes,
                       @JsonProperty("parentAttributes") List<String> parentAttributes,
                       @JsonProperty("columns") Map<String, Set<String>> columns,
                       @JsonProperty("filter") Filter<Column> filter,
                       @JsonProperty("includeMetadata") boolean includeMetadata,
                       @JsonProperty("sort") List<ColumnOrder> sort,
                       @JsonProperty("pagination") @Nullable Pagination pagination,
                       @JsonProperty("parentsCardinality") boolean parentsCardinality,
                       @JsonProperty("scopes") Set<String> scopes) {
        this.requiredAttributes = ImmutableSet.copyOf(requiredAttributes);
        this.parentAttributes = ImmutableSet.copyOf(parentAttributes);
        this.columns = ImmutableMap.copyOf(columns);
        this.filter = filter;
        this.includeMetadata = includeMetadata;
        this.sort = ImmutableList.copyOf(sort);
        this.pagination = Optional.ofNullable(pagination);
        this.scopes = ImmutableSet.copyOf(scopes);
        this.parentsCardinality = parentsCardinality;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ExportQuery that = (ExportQuery) o;
        return includeMetadata == that.includeMetadata &&
                Objects.equals(requiredAttributes, that.requiredAttributes) &&
                Objects.equals(parentAttributes, that.parentAttributes) &&
                Objects.equals(columns, that.columns) &&
                Objects.equals(filter, that.filter) &&
                Objects.equals(sort, that.sort) &&
                Objects.equals(scopes, that.scopes) &&
                Objects.equals(parentsCardinality, that.parentsCardinality) &&
                Objects.equals(pagination, that.pagination);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requiredAttributes, parentAttributes, columns, filter, includeMetadata, sort, pagination, scopes);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requiredAttributes", requiredAttributes)
                .add("parentAttributes", parentAttributes)
                .add("columns", columns)
                .add("filter", filter)
                .add("includeMetadata", includeMetadata)
                .add("sort", sort)
                .add("pagination", pagination)
                .add("scopes", scopes)
                .add("parentsCardinality", parentsCardinality)
                .toString();
    }

}
