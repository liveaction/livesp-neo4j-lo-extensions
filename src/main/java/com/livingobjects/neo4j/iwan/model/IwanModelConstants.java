package com.livingobjects.neo4j.iwan.model;

import com.livingobjects.cosmos.shared.model.GraphLinkLabel;
import com.livingobjects.cosmos.shared.model.GraphNodeLabel;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

public final class IwanModelConstants {
    public static final RelationshipType LINK_ATTRIBUTE = DynamicRelationshipType.withName(GraphLinkLabel.LINK_ATTRIBUTE);
    public static final RelationshipType LINK_CONNECT = DynamicRelationshipType.withName(GraphLinkLabel.LINK_CONNECT);
    public static final RelationshipType LINK_PARENT = DynamicRelationshipType.withName(GraphLinkLabel.LINK_PARENT);
    public static final Label LABEL_ATTRIBUTE = DynamicLabel.label(GraphNodeLabel.ATTRIBUTE);
    public static final Label LABEL_SCOPE = DynamicLabel.label(GraphNodeLabel.SCOPE);
    public static final Label LABEL_PLANET = DynamicLabel.label(GraphNodeLabel.PLANET);
}
