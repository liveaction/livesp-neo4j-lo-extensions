package com.livingobjects.neo4j.loader;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.header.HeaderElement;

import java.util.Collection;

final class LineMappingStrategy extends CsvMappingStrategy {
    final Scope lineScope;
    private final ImmutableSet<String> empties;

    LineMappingStrategy(Scope scope, ImmutableMap<String, Integer> columnIndexes,
                        ImmutableMultimap<String, HeaderElement> mapping, Collection<String> empties) {
        super(columnIndexes, mapping);
        this.lineScope = scope;
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
