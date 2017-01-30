package com.livingobjects.neo4j.model.schema.relationships;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;

public final class Relationships {

    public enum Direction {

        incoming(org.neo4j.graphdb.Direction.INCOMING), outgoing(org.neo4j.graphdb.Direction.OUTGOING);

        public final org.neo4j.graphdb.Direction neo4jDirection;

        Direction(org.neo4j.graphdb.Direction neo4jDirection) {
            this.neo4jDirection = neo4jDirection;
        }

    }

    public final String type;

    public final Direction direction;

    public final boolean replace;

    public final ImmutableSet<Relationship> relationships;

    public Relationships(String type, Direction direction, boolean replace, ImmutableSet<Relationship> relationships) {
        this.type = type;
        this.direction = direction;
        this.replace = replace;
        this.relationships = relationships;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Relationships that = (Relationships) o;
        return replace == that.replace &&
                Objects.equals(type, that.type) &&
                direction == that.direction &&
                Objects.equals(relationships, that.relationships);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, direction, replace, relationships);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("direction", direction)
                .add("replace", replace)
                .add("relationships", relationships)
                .toString();
    }
}
