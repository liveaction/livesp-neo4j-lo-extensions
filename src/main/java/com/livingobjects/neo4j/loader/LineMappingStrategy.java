package com.livingobjects.neo4j.loader;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.GLOBAL_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.ID;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SP_SCOPE;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.TAG;

public final class LineMappingStrategy {

    final CsvMappingStrategy strategy;

    public final String[] line;

    LineMappingStrategy(CsvMappingStrategy strategy, String[] line) {
        this.strategy = strategy;
        this.line = line;
    }

    Scope guessElementScopeInLine(MetaSchema metaSchema, String keyAttribute) {
        return tryToGuessElementScopeInLine(metaSchema, keyAttribute)
                .orElseThrow(() -> new IllegalStateException(String.format("Unable to find a scope in the line to import '%s'.", keyAttribute)));
    }

    String guessScopeAttributeInLine(MetaSchema metaSchema, String keyAttribute) {
        ImmutableSet<String> authorizedScopes = metaSchema.getAuthorizedScopes(keyAttribute);
        if (metaSchema.isOverridable(keyAttribute)) {
            return guessScopeAttribute(keyAttribute, metaSchema.getScopeTypes());
        } else {
            if (authorizedScopes.isEmpty()) {
                return GLOBAL_SCOPE.attribute;
            } else {
                if (authorizedScopes.size() == 1) {
                    return authorizedScopes.iterator().next();
                } else {
                    return guessScopeAttribute(keyAttribute, authorizedScopes);
                }
            }
        }
    }

    Optional<Scope> tryToGuessElementScopeInLine(MetaSchema metaSchema, String keyAttribute) {
        String scopeAttribute = guessScopeAttributeInLine(metaSchema, keyAttribute);
        return readScopeFromLine(scopeAttribute);
    }

    private String guessScopeAttribute(String keyAttribute, ImmutableSet<String> parentScopes) {
        Optional<Integer> scopeColumnIndex = strategy.tryColumnIndex(keyAttribute, SCOPE);
        String parentScopeName;
        if (!scopeColumnIndex.isPresent()) {
            throw new IllegalStateException(String.format("Column '%s.%s' is required to import '%s'.", keyAttribute, SCOPE, keyAttribute));
        } else {
            String scopeNameInLine = line[scopeColumnIndex.get()];
            parentScopeName = Strings.isNullOrEmpty(scopeNameInLine) ? "global" : scopeNameInLine;
        }
        return findScopeFromName(parentScopes, parentScopeName)
                .orElseThrow(() -> new IllegalStateException(String.format("Unable to import '%s' into scope '%s'. Should be one of '%s'", keyAttribute, parentScopeName, parentScopes)));
    }

    private Optional<String> findScopeFromName(ImmutableSet<String> parentScopes, String scopeName) {
        return parentScopes.stream().filter(s -> s.endsWith(":" + scopeName)).findFirst();
    }

    private Optional<Scope> readScopeFromLine(String parentScopeAttribute) {
        if (parentScopeAttribute.equals(SP_SCOPE.attribute)) {
            return Optional.of(SP_SCOPE);
        } else if (parentScopeAttribute.equals(GLOBAL_SCOPE.attribute)) {
            return Optional.of(GLOBAL_SCOPE);
        } else if (strategy.hasKeyType(parentScopeAttribute)) {
            String tag = line[strategy.getColumnIndex(parentScopeAttribute, TAG)];
            if (tag != null && !tag.isEmpty()) {
                String id = line[strategy.getColumnIndex(parentScopeAttribute, ID)];
                return Optional.of(new Scope(id, tag));
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

}
