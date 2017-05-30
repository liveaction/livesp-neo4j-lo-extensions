package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.livingobjects.neo4j.model.header.HeaderElement;

public final class LineMappingStrategy extends IwanMappingStrategy {
    public final Scope scope;

    LineMappingStrategy(Scope scope, ImmutableMap<String, Integer> columnIndexes, ImmutableMultimap<String, HeaderElement> mapping) {
        super(columnIndexes, mapping);
        this.scope = scope;
    }
}
