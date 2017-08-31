package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.header.HeaderElement;

import javax.annotation.Nullable;
import java.util.Collection;

public final class LineMappingStrategy extends IwanMappingStrategy {
    @Nullable
    public final Scope scope;
    private final ImmutableSet<String> empties;

    LineMappingStrategy(@Nullable Scope scope, ImmutableMap<String, Integer> columnIndexes,
                        ImmutableMultimap<String, HeaderElement> mapping, Collection<String> empties) {
        super(columnIndexes, mapping);
        this.scope = scope;
        this.empties = ImmutableSet.copyOf(empties);
    }

    @Override
    boolean hasKeyType(String keytype) {
        return super.hasKeyType(keytype) || empties.contains(keytype);
    }

    @Override
    int getColumnIndex(String keyType, String property) {
        if (empties.contains(keyType))
            return -1;
        else
            return super.getColumnIndex(keyType, property);
    }
}
