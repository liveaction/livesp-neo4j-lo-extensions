package com.livingobjects.neo4j.iwan.model.schema.model;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import java.util.Objects;

public final class SchemaTemplate {

    public final String name;

    public final SchemaVersion version;

    public final Node templateNode;

    public final ImmutableMap<String, Graph> sections;

    public SchemaTemplate(String name, SchemaVersion version, Node templateNode, ImmutableMap<String, Graph> sections) {
        this.name = name;
        this.version = version;
        this.templateNode = templateNode;
        this.sections = sections;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaTemplate that = (SchemaTemplate) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(version, that.version) &&
                Objects.equals(templateNode, that.templateNode) &&
                Objects.equals(sections, that.sections);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, version, templateNode, sections);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("version", version)
                .toString();
    }
}
