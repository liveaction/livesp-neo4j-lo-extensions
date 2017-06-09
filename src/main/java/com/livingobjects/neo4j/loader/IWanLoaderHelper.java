package com.livingobjects.neo4j.loader;

import com.google.common.collect.Maps;

import java.util.Set;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.TAG;

public final class IWanLoaderHelper {

    private static final String CLASS_SCOPE = "class:scope";

    public static Scope findScopeValue(IwanMappingStrategy strategy, Set<String> scopeTypes, String[] line) {
        return strategy.getAllElementsType().stream()
                .filter(scopeTypes::contains)
                .map(kt -> Maps.immutableEntry(line[strategy.getColumnIndex(kt, ID)], line[strategy.getColumnIndex(kt, TAG)]))
                .filter(e -> !e.getValue().isEmpty())
                .map(idx -> new Scope(idx.getKey(), idx.getValue()))
                .findAny()
                .orElseGet(() -> {
                    if (strategy.hasKeyType(CLASS_SCOPE)) {
                        String tag = line[strategy.getColumnIndex(CLASS_SCOPE, TAG)];
                        if (tag != null && !tag.isEmpty()) {
                            String id = line[strategy.getColumnIndex(CLASS_SCOPE, ID)];
                            return new Scope(id, tag);
                        }
                    }
                    return GLOBAL_SCOPE;
                });
    }
}
