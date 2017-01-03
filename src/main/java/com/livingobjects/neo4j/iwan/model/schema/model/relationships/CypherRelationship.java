package com.livingobjects.neo4j.iwan.model.schema.model.relationships;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.iwan.model.schema.model.Property;

import java.util.Objects;

public final class CypherRelationship extends Relationship {

    public final String cypher;

    CypherRelationship(ImmutableSet<Property> properties, String cypher) {
        super(properties);
        this.cypher = cypher;
    }

    @Override
    public void visit(Visitor visitor) {
        visitor.cypher(properties, cypher);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CypherRelationship that = (CypherRelationship) o;
        return Objects.equals(cypher, that.cypher);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cypher);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("cypher", cypher)
                .toString();
    }
}
