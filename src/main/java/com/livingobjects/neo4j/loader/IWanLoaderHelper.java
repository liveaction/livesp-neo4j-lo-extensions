package com.livingobjects.neo4j.loader;

import com.google.common.collect.Maps;

import java.util.Set;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

public final class IWanLoaderHelper {

    private static final String CLASS_SCOPE = "class:scope";

    public static Scope findScopeValue(IwanMappingStrategy strategy, Set<String> scopeTypes, String[] line) {
        if (strategy.hasKeyType(CLASS_SCOPE)) {
            String tag = line[strategy.getColumnIndex(CLASS_SCOPE, TAG)];
            if (tag != null && !tag.isEmpty()) {
                String id = line[strategy.getColumnIndex(CLASS_SCOPE, ID)];
                return new Scope(id, tag);
            }
        }
        return strategy.getAllElementsType().stream()
                .filter(scopeTypes::contains)
                .map(kt -> Maps.immutableEntry(line[strategy.getColumnIndex(kt, ID)], line[strategy.getColumnIndex(kt, TAG)]))
                .filter(e -> !e.getValue().isEmpty())
                .findAny().map(idx -> new Scope(idx.getKey(), idx.getValue()))
                .orElse(GLOBAL_SCOPE);
    }
}
