package com.livingobjects.neo4j.loader;

import com.google.common.base.MoreObjects;
import com.google.common.base.Splitter;

import java.util.Map;
import java.util.Objects;

public class Scope {
    private static final String CLASS = "class";

    public final String id;
    public final String tag;
    public final String attribute;

    public Scope(String id, String tag) {
        this.id = id;
        this.tag = tag;
        Map<String, String> components = Splitter.on(',').withKeyValueSeparator('=').split(tag);
        String type = components.get(CLASS);
        String name = components.get(type);

        if (name == null)
            this.attribute = CLASS + ':' + type;
        else
            this.attribute = type + ':' + name;
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
