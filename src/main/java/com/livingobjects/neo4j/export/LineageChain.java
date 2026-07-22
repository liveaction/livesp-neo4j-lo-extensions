package com.livingobjects.neo4j.export;

import com.livingobjects.neo4j.model.export.Lineage;
import org.neo4j.graphdb.Relationship;

import java.util.Arrays;
import java.util.List;

/**
 * A persistent (structurally-shared) chain of lineages joined by relationships, extended one relationship hop
 * at a time.
 * Appending a hop is O(1) (one new node pointing at its parent) instead of copying the whole accumulated
 * List&lt;Lineage&gt;/List&lt;Relationship&gt; pair on every extension, which matters once a hop has wide fan-out
 * (many matching relationships per line).
 */
public final class LineageChain {
    public final Lineage lineage;
    public final Relationship relationshipFromParent;
    public final LineageChain parent;
    public final int size;

    private LineageChain(Lineage lineage, Relationship relationshipFromParent, LineageChain parent, int size) {
        this.lineage = lineage;
        this.relationshipFromParent = relationshipFromParent;
        this.parent = parent;
        this.size = size;
    }

    public static LineageChain first(Lineage lineage) {
        return new LineageChain(lineage, null, null, 1);
    }

    public LineageChain append(Lineage lineage, Relationship relationship) {
        return new LineageChain(lineage, relationship, this, size + 1);
    }

    public List<Lineage> toLineageList() {
        Lineage[] result = new Lineage[size];
        LineageChain current = this;
        for (int idx = size - 1; idx >= 0; idx--) {
            result[idx] = current.lineage;
            current = current.parent;
        }
        return Arrays.asList(result);
    }

    public List<Relationship> toRelationshipList() {
        Relationship[] result = new Relationship[size - 1];
        LineageChain current = this;
        for (int idx = size - 2; idx >= 0; idx--) {
            result[idx] = current.relationshipFromParent;
            current = current.parent;
        }
        return Arrays.asList(result);
    }
}