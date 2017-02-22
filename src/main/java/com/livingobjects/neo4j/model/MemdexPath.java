package com.livingobjects.neo4j.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.livingobjects.neo4j.model.iwan.IwanModelConstants;
import com.livingobjects.neo4j.model.iwan.Labels;

import java.util.Map;
import java.util.Objects;

public final class MemdexPath {
    public final String segment;
    public final String template;
    public final ImmutableList<String> attributes;
    public final ImmutableList<Map<String, Object>> counters;
    public final ImmutableList<MemdexPath> children;

    private MemdexPath(String segment, String template, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters, ImmutableList<MemdexPath> children) {
        long neTypeCount = getNeTypeCount(attributes);
        if (neTypeCount != 1) {
            throw new IllegalArgumentException("MemdexPath must have one and only one " + Labels.NETWORK_ELEMENT.name() + " attribute !");
        }
        this.segment = segment;
        this.template = template;
        this.attributes = attributes;
        this.counters = counters;
        this.children = children;
    }


    public static MemdexPath build(String segment, String template, ImmutableList<String> attributes) {
        return new MemdexPath(segment, template, attributes, ImmutableList.of(), ImmutableList.of());
    }

    public static MemdexPath build(String segment, String template, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters) {
        return new MemdexPath(segment, template, attributes, counters, ImmutableList.of());
    }

    public static MemdexPath build(String segment, String template, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters, ImmutableList<MemdexPath> children) {
        return new MemdexPath(segment, template, attributes, counters, children);
    }

    public static MemdexPath build(String segment, String template, ImmutableList<String> attributes, ImmutableList<Map<String, Object>> counters, MemdexPath children) {
        return new MemdexPath(segment, template, attributes, counters, ImmutableList.of(children));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MemdexPath that = (MemdexPath) o;
        return Objects.equals(segment, that.segment) &&
                Objects.equals(template, that.template) &&
                Objects.equals(attributes, that.attributes) &&
                Objects.equals(counters, that.counters) &&
                Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(segment, template, attributes, counters, children);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .addValue(template)
                .toString();
    }

    private static long getNeTypeCount(ImmutableList<String> attributes) {
        return attributes.stream().filter(b -> IwanModelConstants.KEY_TYPES.contains(b.substring(0, b.indexOf(':')))).count();
    }
}
