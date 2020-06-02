package com.livingobjects.neo4j.model.iwan;

import org.neo4j.graphdb.RelationshipType;

public class RelationshipTypes {


    public static final RelationshipType ATTRIBUTE = RelationshipType.withName("Attribute");
    public static final RelationshipType CROSS_ATTRIBUTE = RelationshipType.withName("CrossAttribute");
    public static final RelationshipType CONNECT = RelationshipType.withName("Connect");
    public static final RelationshipType PARENT = RelationshipType.withName("Parent");
    public static final RelationshipType PROVIDED = RelationshipType.withName("Provided");
    public static final RelationshipType MEMDEXPATH = RelationshipType.withName("MdxPath");
    public static final RelationshipType EXTEND = RelationshipType.withName("Extend");
    public static final RelationshipType APPLIED_TO = RelationshipType.withName("AppliedTo");
    public static final RelationshipType VAR = RelationshipType.withName("Var");

}
