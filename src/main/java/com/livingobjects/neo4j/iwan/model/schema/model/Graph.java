package com.livingobjects.neo4j.iwan.model.schema.model;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.Objects;
import java.util.Set;

public final class Graph {

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        Set<Node> nodes = Sets.newHashSet();
        Set<Relationship> rels = Sets.newHashSet();

        public Builder node(Node node) {
            nodes.add(node);
            return this;
        }

        public Builder relationship(Node from, Node to, String type) {
            return relationship(from, to, type, ImmutableSet.of());
        }

        public Builder relationship(Node from, Node to, String type, ImmutableSet<Property> properties) {
            nodes.add(from);
            nodes.add(to);
            rels.add(new Relationship(from, to, type, properties));
            return this;
        }

        public Graph build() {
            return new Graph(ImmutableSet.copyOf(nodes), ImmutableSet.copyOf(rels));
        }
    }

    public final ImmutableSet<Node> nodes;

    public final ImmutableSet<Relationship> relationships;

    public Graph(ImmutableSet<Node> nodes, ImmutableSet<Relationship> relationships) {
        this.nodes = nodes;
        this.relationships = relationships;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Graph graph = (Graph) o;
        return Objects.equals(nodes, graph.nodes) &&
                Objects.equals(relationships, graph.relationships);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodes, relationships);
    }
}
