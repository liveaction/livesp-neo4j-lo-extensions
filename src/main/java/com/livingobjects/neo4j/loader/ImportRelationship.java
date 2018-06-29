package com.livingobjects.neo4j.loader;

import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.RelationshipType;

import javax.annotation.Nullable;

public enum ImportRelationship {

    CrossAttribute(RelationshipTypes.CROSS_ATTRIBUTE);

    public final RelationshipType relationshipType;

    public static @Nullable
    ImportRelationship of(String name) {
        for (ImportRelationship importRelationship : values()) {
            if (name.equalsIgnoreCase(importRelationship.name())) {
                return importRelationship;
            }
        }
        return null;
    }

    ImportRelationship(RelationshipType relationshipType) {
        this.relationshipType = relationshipType;
    }

}
