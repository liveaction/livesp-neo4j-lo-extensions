package com.livingobjects.neo4j.model.iwan;

import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.loader.Scope;

public final class GraphModelConstants {
    public static final String LINK_PROP_SPECIALIZER = "specializer";

    public static final String SCOPE_GLOBAL_ATTRIBUTE = "scope:global";
    public static final String SCOPE_GLOBAL_TAG = "class=scope,scope=global";
    public static final String SCOPE_SP_TAG = "class=scope,scope=sp";

    public static final Scope GLOBAL_SCOPE = new Scope("global", SCOPE_GLOBAL_TAG);
    public static final Scope SP_SCOPE = new Scope("sp", SCOPE_SP_TAG);

    public static final char KEYTYPE_SEPARATOR = ':';
    public static final String LABEL_TYPE = "labelType";
    public static final String SCOPE = "scope";
    public static final ImmutableSet<String> IMPORTABLE_KEY_TYPES = ImmutableSet.of("cluster", "neType");
    public static final ImmutableSet<String> KEY_TYPES = ImmutableSet.of("cluster", "neType", LABEL_TYPE, SCOPE);
    public static final String CARDINALITY_MULTIPLE = "0..n";
    public static final String CARDINALITY_UNIQUE_PARENT = "1..1";
    public static final String _TYPE = "_type";
    public static final String _DEFAULT_SCOPE = "_defaultScope";
    public static final String ID = "id";
    public static final String VERSION = "version";
    public static final String NAME = "name";
    public static final String MANAGED = "managed";
    public static final String DESCRIPTION = "description";
    public static final String PATH = "path";
    public static final String CARDINALITY = "cardinality";
    public static final String TAG = "tag";
    public static final String SCHEMA = "schema";
    public static final String CREATED_AT = "createdAt";
    public static final String CREATED_BY = "createdBy";
    public static final String UPDATED_AT = "updatedAt";
    public static final String UPDATED_BY = "updatedBy";
    public static final String _OVERRIDABLE = "_overridable";
    public static final String OVERRIDE = "override";
    public static final String CONTEXT = "context";

    public static final ImmutableSet<String> RESERVED_PROPERTIES = ImmutableSet.of(TAG, SCOPE, _TYPE, CREATED_AT, UPDATED_AT, CREATED_BY, UPDATED_BY);

}
