package com.livingobjects.neo4j.iwan.model.schema.model.relationships;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.iwan.model.schema.model.Property;

import java.util.Objects;

public final class NodeRelationship extends Relationship {

    public final String node;

    NodeRelationship(ImmutableSet<Property> properties, String node) {
        super(properties);
        this.node = node;
    }

    @Override
    public void visit(Visitor visitor) {
        visitor.node(properties, node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeRelationship that = (NodeRelationship) o;
        return Objects.equals(node, that.node) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, properties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("node", node)
                .add("properties", properties)
                .toString();
    }
}
