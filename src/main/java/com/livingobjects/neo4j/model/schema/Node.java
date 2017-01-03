package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.schema.relationships.Relationships;

import java.util.Objects;
import java.util.Optional;

public final class Node {

    public final Optional<String> id;

    public final ImmutableList<String> labels;

    public final ImmutableMap<String, String> keys;

    public final ImmutableSet<Property> properties;

    public final ImmutableSet<Relationships> relationships;

    public Node(Optional<String> id, ImmutableList<String> labels, ImmutableMap<String, String> keys, ImmutableSet<Property> properties, ImmutableSet<Relationships> relationships) {
        this.id = id;
        this.labels = labels;
        this.keys = keys;
        this.properties = properties;
        this.relationships = relationships;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(id, node.id) &&
                Objects.equals(labels, node.labels) &&
                Objects.equals(keys, node.keys) &&
                Objects.equals(properties, node.properties) &&
                Objects.equals(relationships, node.relationships);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, labels, keys, properties, relationships);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("labels", labels)
                .add("keys", keys)
                .add("properties", properties)
                .add("relationships", relationships)
                .toString();
    }
}
