package com.livingobjects.neo4j.iwan.model;

import com.google.common.collect.ImmutableSet;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public final class IwanModelConstants {
    public static final RelationshipType LINK_ATTRIBUTE = DynamicRelationshipType.withName("Attribute");
    public static final RelationshipType LINK_CROSS_ATTRIBUTE = DynamicRelationshipType.withName("CrossAttribute");
    public static final RelationshipType LINK_CONNECT = DynamicRelationshipType.withName("Connect");
    public static final RelationshipType LINK_PARENT = DynamicRelationshipType.withName("Parent");
    public static final Label LABEL_ATTRIBUTE = DynamicLabel.label("Attribute");
    public static final Label LABEL_SCOPE = DynamicLabel.label("Scope");
    public static final Label LABEL_PLANET = DynamicLabel.label("Planet");
    public static final Label LABEL_NETWORK_ELEMENT = DynamicLabel.label("NetworkElement");

    public static final String SCOPE_GLOBAL_ATTRIBUTE = "scope:global";
    public static final String SCOPE_GLOBAL_TAG = "class=scope,scope=global";
    public static final char KEYTYPE_SEPARATOR = ':';
    public static final ImmutableSet<String> KEY_TYPES = ImmutableSet.of("cluster", "neType", "labelType", "scope");
    public static final String CARDINALITY_MULTIPLE = "0..n";
    public static final String _TYPE = "_type";
    public static final String NAME = "name";
    public static final String CARDINALITY = "cardinality";
    public static final String TAG = "tag";
    public static final String UPDATED_AT = "updatedAt";
    public static final String _OVERRIDABLE = "_overridable";
    public static final String _SCOPE = "_scope";
}
