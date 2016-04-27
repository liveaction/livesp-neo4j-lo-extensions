package com.livingobjects.neo4j.iwan.model;

import com.google.common.collect.ImmutableSet;

public final class GraphModelConstants {
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
