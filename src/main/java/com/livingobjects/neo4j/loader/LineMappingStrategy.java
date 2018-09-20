package com.livingobjects.neo4j.loader;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;

public final class LineMappingStrategy {

    public final CsvMappingStrategy strategy;

    public final String[] line;

    public LineMappingStrategy(CsvMappingStrategy strategy, String[] line) {
        this.strategy = strategy;
        this.line = line;
    }

    public Scope guessElementScope(MetaSchema metaSchema, String keyAttribute) {
        ImmutableSet<String> parentScopes = metaSchema.getParentScopes(keyAttribute);
        if (metaSchema.isOverridable(keyAttribute)) {
            String parentScope = guessSelectedScope(keyAttribute, metaSchema.scopeTypes, false);
            return readScopeFromLine(keyAttribute, parentScope);
        } else {
            if (parentScopes.isEmpty()) {
                return GLOBAL_SCOPE;
            } else {
                if (parentScopes.size() == 1) {
                    String parentScope = parentScopes.iterator().next();
                    return readScopeFromLine(keyAttribute, parentScope);
                } else {
                    String parentScope = guessSelectedScope(keyAttribute, parentScopes, true);
                    return readScopeFromLine(keyAttribute, parentScope);
                }
            }
        }
    }

    private String guessSelectedScope(String keyAttribute, ImmutableSet<String> parentScopes, boolean required) {
        int scopeColumnIndex = strategy.getColumnIndex(keyAttribute, SCOPE);
        String parentScope;
        if (scopeColumnIndex < 0) {
            if (required) {
                throw new IllegalStateException(String.format("Column '%s.%s' is required to import '%s'.", keyAttribute, SCOPE, keyAttribute));
            } else {
                parentScope = "global";
            }
        } else {
            parentScope = line[scopeColumnIndex];
            parentScope = Strings.isNullOrEmpty(parentScope) ? "global" : parentScope;
        }
        if (parentScopes.contains(parentScope)) {
            return parentScope;
        } else {
            throw new IllegalStateException(String.format("Unauthorized '%s.%s' to import '%s'. '%s' not found in '%s'", keyAttribute, SCOPE, keyAttribute, parentScope, parentScopes));
        }
    }

    private Scope readScopeFromLine(String keyAttribute, String parentScope) {
        if (strategy.hasKeyType(parentScope)) {
            String tag = line[strategy.getColumnIndex(parentScope, TAG)];
            if (tag != null && !tag.isEmpty()) {
                String id = line[strategy.getColumnIndex(parentScope, ID)];
                return new Scope(id, tag);
            } else {
                throw new IllegalStateException(String.format("Column '%s.tag' is required for columns '%s' and is not found.", parentScope, keyAttribute));
            }
        } else {
            throw new IllegalStateException(String.format("Parent scope '%s' is required for columns '%s' and is not found.", parentScope, keyAttribute));
        }
    }

}
