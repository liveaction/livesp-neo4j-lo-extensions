package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Set;

public final class Planet {

    public String name;

    public String path;

    public Attribute keyAttribute;

    public Set<Attribute> attributes;

    public boolean global;

    public Planet() {
    }

    public Planet(
            String name,
            String path,
            Attribute keyAttribute,
            Set<Attribute> attributes,
            boolean global) {
        this.name = name;
        this.path = path;
        this.keyAttribute = keyAttribute;
        this.attributes = attributes;
        this.global = global;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Attribute getKeyAttribute() {
        return keyAttribute;
    }

    public void setKeyAttribute(Attribute keyAttribute) {
        this.keyAttribute = keyAttribute;
    }

    public Set<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<Attribute> attributes) {
        this.attributes = attributes;
    }

    public boolean isGlobal() {
        return global;
    }

    public void setGlobal(boolean global) {
        this.global = global;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Planet planet = (Planet) o;
        return global == planet.global &&
                Objects.equals(name, planet.name) &&
                Objects.equals(path, planet.path) &&
                Objects.equals(keyAttribute, planet.keyAttribute) &&
                Objects.equals(attributes, planet.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, path, keyAttribute, attributes, global);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("path", path)
                .add("keyAttribute", keyAttribute)
                .add("nodesByKeys", attributes)
                .add("global", global)
                .toString();
    }
}
