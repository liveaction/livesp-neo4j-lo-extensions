package com.livingobjects.neo4j.model.iwan;

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
    public static final RelationshipType LINK_PROVIDED = DynamicRelationshipType.withName("Provided");
    public static final RelationshipType LINK_MEMDEXPATH = DynamicRelationshipType.withName("MdxPath");
    public static final String LINK_PROP_SPECIALIZER = "specializer";
    public static final Label LABEL_ATTRIBUTE = DynamicLabel.label("Attribute");
    public static final Label LABEL_SCOPE = DynamicLabel.label("Scope");
    public static final Label LABEL_PLANET = DynamicLabel.label("Planet");
    public static final Label LABEL_NETWORK_ELEMENT = DynamicLabel.label("NetworkElement");
    public static final Label LABEL_ELEMENT = DynamicLabel.label("Element");

    public static final String SCOPE_GLOBAL_ATTRIBUTE = "scope:global";
    public static final String SCOPE_GLOBAL_TAG = "class=scope,scope=global";
    public static final char KEYTYPE_SEPARATOR = ':';
    public static final ImmutableSet<String> KEY_TYPES = ImmutableSet.of("cluster", "neType", "labelType", "scope");
    public static final String CARDINALITY_MULTIPLE = "0..n";
    public static final String _TYPE = "_type";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String PATH = "path";
    public static final String CREATED_AT = "createdAt";
    public static final String CARDINALITY = "cardinality";
    public static final String TAG = "tag";
    public static final String UPDATED_AT = "updatedAt";
    public static final String _OVERRIDABLE = "_overridable";
    public static final String _SCOPE = "_scope";
}