package com.livingobjects.neo4j.model.export.query;

import org.neo4j.graphdb.Direction;

public class RelationshipQuery {

    public final Direction direction;
    public final String type;

    /**
     * Creates a new RelationshipQuery
     * @param direction the direction. Can be either INCOMING or OUTGOING
     * @param type value of the type attribute
     * @throws IllegalArgumentException if Direction is BOTH
     */
    public RelationshipQuery(Direction direction, String type) {
        if(direction == Direction.BOTH) {
            throw new IllegalArgumentException("Direction 'BOTH' is not supported");
        }
        this.direction = direction;
        this.type = type;
    }
}
