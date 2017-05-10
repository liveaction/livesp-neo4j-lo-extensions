package com.livingobjects.neo4j.model.result;

import java.util.Objects;

public final class TypedScope {

    public final String scope;

    public final String type;

    public TypedScope(String typeAtScope) {
        String[] split = typeAtScope.split("@");
        if (split.length != 2) {
            throw new IllegalArgumentException("Coulnd not read Typed Scope from '" + typeAtScope + "'. Must be of the form 'type@scope'.");
        }
        this.scope = split[1];
        this.type = split[0];
    }

    public TypedScope(String scope, String type) {
        this.scope = scope;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TypedScope that = (TypedScope) o;
        return Objects.equals(scope, that.scope) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scope, type);
    }

    @Override
    public String toString() {
        return type + '@' + scope;
    }

}
