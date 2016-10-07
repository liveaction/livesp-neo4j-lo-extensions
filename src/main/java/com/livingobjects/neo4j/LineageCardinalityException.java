package com.livingobjects.neo4j;

public class LineageCardinalityException extends Throwable {

    public final ExportCSVExtension.Lineage lineage;
    public final String existingNode;
    public final String parentNode;

    public LineageCardinalityException(ExportCSVExtension.Lineage lineage, String existingNode, String parentNode) {
        this.lineage = lineage;
        this.existingNode = existingNode;
        this.parentNode = parentNode;
    }
}
