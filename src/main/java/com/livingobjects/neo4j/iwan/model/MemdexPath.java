package com.livingobjects.neo4j.iwan.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;

import java.util.Map;
import java.util.Objects;

public final class MemdexPath {
    public final String name;
    public final ImmutableList<String> attributes;
    public final ImmutableList<Map<String, Object>> counters;
    public final ImmutableList<MemdexPath> children;

    private MemdexPath(String name, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters, ImmutableList<MemdexPath> children) {
        long neTypeCount = getNeTypeCount(attributes);
        if (neTypeCount != 1) {
            throw new IllegalArgumentException("MemdexPath must have one and only one " + IwanModelConstants.LABEL_NETWORK_ELEMENT.name() + " attribute !");
        }
        this.name = name;
        this.attributes = attributes;
        this.counters = counters;
        this.children = children;
    }


    public static MemdexPath build(String name, ImmutableList<String> attributes) {
        return new MemdexPath(name, attributes, ImmutableList.of(), ImmutableList.of());
    }

    public static MemdexPath build(String name, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters) {
        return new MemdexPath(name, attributes, counters, ImmutableList.of());
    }

    public static MemdexPath build(String name, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters, ImmutableList<MemdexPath> children) {
        return new MemdexPath(name, attributes, counters, children);
    }

    public static MemdexPath build(String name, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters, MemdexPath children) {
        return new MemdexPath(name, attributes, counters, ImmutableList.of(children));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemdexPath that = (MemdexPath) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(attributes, that.attributes) &&
                Objects.equals(counters, that.counters) &&
                Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, attributes, counters, children);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("nodesByKeys", attributes)
                .add("counters", counters)
                .add("children", children)
                .toString();
    }

    private static long getNeTypeCount(ImmutableList<String> attributes) {
        return attributes.stream().filter(b -> IwanModelConstants.KEY_TYPES.contains(b.substring(0, b.indexOf(':')))).count();
    }
}
