package com.livingobjects.neo4j;

import com.livingobjects.neo4j.model.export.Lineage;

public class LineageCardinalityException extends Throwable {

    public final Lineage lineage;
    public final String existingNode;
    public final String parentNode;

    public LineageCardinalityException(Lineage lineage, String existingNode, String parentNode) {
        this.lineage = lineage;
        this.existingNode = existingNode;
        this.parentNode = parentNode;
    }
}
