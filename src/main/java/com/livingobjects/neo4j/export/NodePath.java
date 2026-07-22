package com.livingobjects.neo4j.export;

import org.neo4j.graphdb.Node;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A persistent (structurally-shared) chain of (type, node) entries accumulated while walking a lineage upward.
 * Extending it is O(1) (one new node pointing at its parent) and safe to share across
 * sibling branches (multiple parents, or multiple parent-type groups) without copying, unlike a plain
 * Map&lt;String, Node&gt; that would need a full copy at every recursive step to keep branches independent.
 * The flat Map&lt;String, Node&gt; is only materialized once a branch actually terminates.
 */
public final class NodePath {
    public final String type;
    public final Node node;
    public final NodePath parent;

    private NodePath(String type, Node node, NodePath parent) {
        this.type = type;
        this.node = node;
        this.parent = parent;
    }

    public static NodePath append(NodePath path, String type, Node node) {
        return new NodePath(type, node, path);
    }

    public static boolean containsAll(NodePath path, Set<String> types) {
        for (String type : types) {
            if (!contains(path, type)) {
                return false;
            }
        }
        return true;
    }

    public static boolean contains(NodePath path, String wantedType) {
        for (NodePath p = path; p != null; p = p.parent) {
            if (p.type.equals(wantedType)) {
                return true;
            }
        }
        return false;
    }

    public static Map<String, Node> toMap(NodePath path) {
        Map<String, Node> result = new HashMap<>();
        for (NodePath p = path; p != null; p = p.parent) {
            result.putIfAbsent(p.type, p.node);
        }
        return result;
    }
}
