package com.livingobjects.neo4j.model.export.query;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.codehaus.jackson.annotate.JsonProperty;
import org.neo4j.graphdb.Direction;

public class RelationshipQuery {

    public final Direction direction;
    public final String type;

    /**
     * Creates a new RelationshipQuery
     *
     * @param direction the direction. Can be INCOMING or OUTGOING
     * @param type      value of the type attribute
     * @throws IllegalArgumentException if Direction is BOTH
     */
    public RelationshipQuery(@JsonProperty("direction") Direction direction,
                             @JsonProperty("type") String type) {
        if(direction == Direction.BOTH) {
            throw new IllegalArgumentException("Direction \"BOTH\" is not supported");
        }
        this.direction = direction;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationshipQuery that = (RelationshipQuery) o;
        return direction == that.direction &&
                Objects.equal(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(direction, type);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("direction", direction)
                .add("type", type)
                .toString();
    }
}
