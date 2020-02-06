package com.livingobjects.neo4j.model.export;

public class CrossRelationship {
    public final String destinationType;
    public final String originType;
    public final String type;

    public CrossRelationship(String destinationType, String originType, String type) {
        this.destinationType = destinationType;
        this.originType = originType;
        this.type = type;
    }
}
