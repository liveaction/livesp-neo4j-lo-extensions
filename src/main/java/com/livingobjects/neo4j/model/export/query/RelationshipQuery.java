package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonProperty;
import org.neo4j.graphdb.Direction;

import java.util.List;

public class RelationshipQuery {

    public final Direction direction;
    public final String type;
    public final List<String> propertiesToExport;

    /**
     * Creates a new RelationshipQuery
     *
     * @param direction          the direction. Can be INCOMING or OUTGOING
     * @param type               value of the type attribute
     * @param propertiesToExport properties to export
     * @throws IllegalArgumentException if Direction is BOTH
     */
    public RelationshipQuery(@JsonProperty("direction") Direction direction,
                             @JsonProperty("type") String type,
                             @JsonProperty("propertiesToExport") List<String> propertiesToExport) {
        this.direction = direction;
        this.type = type;
        this.propertiesToExport = propertiesToExport;
    }

    /**
     * Creates a new RelationshipQuery which will export no property
     *
     * @param direction the direction. Can be INCOMING or OUTGOING
     * @param type      value of the type attribute
     */
    public RelationshipQuery(Direction direction, String type) {
        this(direction, type, ImmutableList.of());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationshipQuery that = (RelationshipQuery) o;
        return direction == that.direction &&
                Objects.equal(type, that.type) &&
                Objects.equal(propertiesToExport, that.propertiesToExport);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(direction, type, propertiesToExport);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("direction", direction)
                .add("type", type)
                .add("propertiesToExport", propertiesToExport)
                .toString();
    }
}
