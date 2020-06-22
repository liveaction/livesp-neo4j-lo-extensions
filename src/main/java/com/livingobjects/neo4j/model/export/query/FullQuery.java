package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class FullQuery {

    public final ImmutableList<ExportQuery> exportQueries;
    public final ImmutableList<RelationshipQuery> relationshipQueries;
    // pagination (/!\: not the same as the Pagination in longback-commons)
    public final Optional<Pagination> pagination;
    public final ImmutableList<Pair<Integer, ColumnOrder>> ordersByIndex;

    public FullQuery(@JsonProperty("exportQueries") List<ExportQuery> exportQueries,
                     @JsonProperty("pagination") @Nullable Pagination pagination,
                     @JsonProperty("ordersByIndex") @Nullable List<Pair<Integer, ColumnOrder>> ordersByIndex,
                     @JsonProperty("relationshipQueries") @Nullable List<RelationshipQuery> relationshipQueries) {
        ImmutableList<RelationshipQuery> relations = relationshipQueries == null ? ImmutableList.of() : ImmutableList.copyOf(relationshipQueries);
        ImmutableList<Pair<Integer, ColumnOrder>> orders = ordersByIndex == null ? ImmutableList.of() : ImmutableList.copyOf(ordersByIndex);
        if (exportQueries.size() != relations.size() + 1) {
            throw new IllegalArgumentException(String.format("Error constructing FullQuery: there are %d export queries " +
                            "and %d relationship queries, while there should be exactly one more export query than relationship queries",
                    exportQueries.size(),
                    relations.size()));
        }
        this.exportQueries = ImmutableList.copyOf(exportQueries);
        this.relationshipQueries = ImmutableList.copyOf(relations);
        this.pagination = Optional.ofNullable(pagination);
        this.ordersByIndex = orders;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullQuery fullQuery = (FullQuery) o;
        return Objects.equal(exportQueries, fullQuery.exportQueries) &&
                Objects.equal(relationshipQueries, fullQuery.relationshipQueries) &&
                Objects.equal(pagination, fullQuery.pagination) &&
                Objects.equal(ordersByIndex, fullQuery.ordersByIndex);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(exportQueries, relationshipQueries, pagination, ordersByIndex);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("exportQueries", exportQueries)
                .add("relationshipQueries", relationshipQueries)
                .add("pagination", pagination)
                .add("ordersByIndex", ordersByIndex)
                .toString();
    }
}
