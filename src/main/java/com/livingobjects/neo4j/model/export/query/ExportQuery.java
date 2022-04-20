package com.livingobjects.neo4j.model.export.query;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.export.query.filter.Filter;

import java.util.Map;
import java.util.Objects;
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
    // scopes of the query (values can be <client_id>, global or sp). Allows filtering by planets
    public final ImmutableSet<String> scopes;
    // true if this specific query should not return any results (only used for relationships purposes)
    public final boolean noResult;
    // true if you want to export one line for each parent when an element has several parents of the same type (ex: a Site with several Area)
    public final boolean parentsCardinality;

    public ExportQuery(@JsonProperty("requiredAttributes") Set<String> requiredAttributes,
                       @JsonProperty("parentAttributes") Set<String> parentAttributes,
                       @JsonProperty("columns") Map<String, Set<String>> columns,
                       @JsonProperty("filter") Filter<Column> filter,
                       @JsonProperty("includeMetadata") boolean includeMetadata,
                       @JsonProperty("scopes") Set<String> scopes,
                       @JsonProperty("parentsCardinality") boolean parentsCardinality,
                       @JsonProperty("noResult") boolean noResult) {
        this.requiredAttributes = ImmutableSet.copyOf(requiredAttributes);
        this.parentAttributes = ImmutableSet.copyOf(parentAttributes);
        this.columns = ImmutableMap.copyOf(columns);
        this.filter = filter;
        this.includeMetadata = includeMetadata;
        this.scopes = ImmutableSet.copyOf(scopes);
        this.parentsCardinality = parentsCardinality;
        this.noResult = noResult;
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
                Objects.equals(parentsCardinality, that.parentsCardinality) &&
                Objects.equals(scopes, that.scopes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(requiredAttributes, parentAttributes, columns, filter, includeMetadata, scopes);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("requiredAttributes", requiredAttributes)
                .add("parentAttributes", parentAttributes)
                .add("columns", columns)
                .add("filter", filter)
                .add("includeMetadata", includeMetadata)
                .add("scopes", scopes)
                .add("parentsCardinality", parentsCardinality)
                .add("noResult", scopes)
                .toString();
    }

}
