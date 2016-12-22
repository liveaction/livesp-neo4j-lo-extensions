package com.livingobjects.neo4j.iwan.model.schema.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Objects;

public final class Node {

    public final ImmutableList<String> labels;

    public final ImmutableMap<String, String> keys;

    public final ImmutableSet<Property> properties;

    public Node(ImmutableList<String> labels, ImmutableMap<String, String> keys, ImmutableSet<Property> properties) {
        this.labels = labels;
        this.keys = keys;
        this.properties = properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Node node = (Node) o;
        return Objects.equals(labels, node.labels) &&
                Objects.equals(keys, node.keys) &&
                Objects.equals(properties, node.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(labels, keys, properties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("labels", labels)
                .add("keys", keys)
                .add("properties", properties)
                .toString();
    }
}
