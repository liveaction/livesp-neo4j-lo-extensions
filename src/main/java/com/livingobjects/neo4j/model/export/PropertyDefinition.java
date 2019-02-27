package com.livingobjects.neo4j.model.export;

import com.google.common.base.MoreObjects;

import java.util.Objects;

public final class PropertyDefinition {

    public final String type;

    public final boolean required;

    public PropertyDefinition(String type, boolean required) {
        this.type = type;
        this.required = required;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PropertyDefinition that = (PropertyDefinition) o;
        return required == that.required &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, required);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("required", required)
                .toString();
    }

}
