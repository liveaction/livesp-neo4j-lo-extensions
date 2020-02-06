package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.Optional;

public class FullQuery {

    public final ImmutableList<ExportQuery> exportQueries;
    public final ImmutableList<RelationshipQuery> relationshipQueries;
    // pagination (/!\: not the same as the Pagination in longback-commons)
    public final Optional<Pagination> pagination;

    public FullQuery(@JsonProperty("exportQueries") ImmutableList<ExportQuery> exportQueries,
                     @JsonProperty("pagination") @Nullable Pagination pagination,
                     @JsonProperty("relationshipQueries") ImmutableList<RelationshipQuery> relationshipQueries) {
        if (exportQueries.size() != relationshipQueries.size() + 1) {
            throw new IllegalArgumentException(String.format("Error constructing FullQuery: there are %d export queries " +
                            "and %d relationship queries, while there should be exactly one more export query than relationship queries",
                    exportQueries.size(),
                    relationshipQueries.size()));
        }
        if (exportQueries.stream()
                .map(q -> q.sort)
                .filter(orders -> orders != null && orders.size() > 0)
                .count() > 1) {
            throw new IllegalArgumentException("Error: more than one ExportQuery contains sorting information, this is forbidden");
        }
        this.exportQueries = exportQueries;
        this.relationshipQueries = relationshipQueries;
        this.pagination = Optional.ofNullable(pagination);
    }

    public FullQuery(@JsonProperty("pagination") @Nullable Pagination pagination,
                     @JsonProperty("exportQueries") ImmutableList<ExportQuery> exportQueries) {
        if (exportQueries.size() > 1) {
            throw new IllegalArgumentException(String.format("Error constructing FullQuery: there are %d export queries " +
                            "and no relationship queries, while there should be exactly one more export query than relationship queries",
                    exportQueries.size()));
        }
        this.exportQueries = exportQueries;
        this.relationshipQueries = ImmutableList.of();
        this.pagination = Optional.ofNullable(pagination);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FullQuery fullQuery = (FullQuery) o;
        return Objects.equal(exportQueries, fullQuery.exportQueries) &&
                Objects.equal(relationshipQueries, fullQuery.relationshipQueries) &&
                Objects.equal(pagination, fullQuery.pagination);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(exportQueries, relationshipQueries, pagination);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("exportQueries", exportQueries)
                .add("relationshipQueries", relationshipQueries)
                .add("pagination", pagination)
                .toString();
    }
}
