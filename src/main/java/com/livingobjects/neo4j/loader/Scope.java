package com.livingobjects.neo4j.loader;

import com.google.common.base.MoreObjects;

import java.util.Objects;

public class Scope {

    public final String id;

    public final String tag;

    public Scope(String id, String tag) {
        this.id = id;
        this.tag = tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Scope scope = (Scope) o;
        return Objects.equals(id, scope.id) &&
                Objects.equals(tag, scope.tag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, tag);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("tag", tag)
                .toString();
    }
}
