package com.livingobjects.neo4j.model.iwan;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

public final class Labels {
    public static final Label ATTRIBUTE = DynamicLabel.label("Attribute");
    public static final Label SCOPE = DynamicLabel.label("Scope");
    public static final Label PLANET = DynamicLabel.label("Planet");
    public static final Label NETWORK_ELEMENT = DynamicLabel.label("NetworkElement");
    public static final Label ELEMENT = DynamicLabel.label("Element");
    public static final Label SCHEMA = DynamicLabel.label("Schema");
    public static final Label REALM_TEMPLATE = DynamicLabel.label("RealmTemplate");
    public static final Label PLANET_TEMPLATE = DynamicLabel.label("PlanetTemplate");
    public static final Label SEGMENT = DynamicLabel.label("Segment");
    public static final Label VERSION = DynamicLabel.label("Version");
    public static final Label COUNTER = DynamicLabel.label("Counter");
    public static final Label KPI = DynamicLabel.label("KPI");
}
