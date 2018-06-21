package com.livingobjects.neo4j.model.iwan;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;

public class RelationshipTypes {

    public static final RelationshipType ATTRIBUTE = DynamicRelationshipType.withName("Attribute");
    public static final RelationshipType CROSS_ATTRIBUTE = DynamicRelationshipType.withName("CrossAttribute");
    public static final RelationshipType CONNECT = DynamicRelationshipType.withName("Connect");
    public static final RelationshipType PARENT = DynamicRelationshipType.withName("Parent");
    public static final RelationshipType PROVIDED = DynamicRelationshipType.withName("Provided");
    public static final RelationshipType MEMDEXPATH = DynamicRelationshipType.withName("MdxPath");
    public static final RelationshipType EXTEND = DynamicRelationshipType.withName("Extend");
    public static final RelationshipType APPLIED_TO = DynamicRelationshipType.withName("AppliedTo");
    public static final RelationshipType VAR = DynamicRelationshipType.withName("Var");

}
