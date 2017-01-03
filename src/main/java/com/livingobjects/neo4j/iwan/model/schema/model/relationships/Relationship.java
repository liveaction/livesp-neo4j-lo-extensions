package com.livingobjects.neo4j.iwan.model.schema.model.relationships;

import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.iwan.model.schema.model.Property;

public abstract class Relationship {

    public final ImmutableSet<Property> properties;

    Relationship(ImmutableSet<Property> properties) {
        this.properties = properties;
    }

    public static NodeRelationship node(ImmutableSet<Property> properties, String node) {
        return new NodeRelationship(properties, node);
    }

    public static CypherRelationship cypher(ImmutableSet<Property> properties, String cypher) {
        return new CypherRelationship(properties, cypher);
    }

    public interface Visitor {
        void node(ImmutableSet<Property> properties, String node);

        void cypher(ImmutableSet<Property> properties, String cypher);
    }

    public abstract void visit(Visitor visitor);

}
