package com.livingobjects.neo4j.iwan.model.schema;

import java.util.Objects;

public final class Attribute {

    public String type;

    public String name;

    public String specializer;

    public Attribute() {
    }

    public Attribute(String type, String name, String specializer) {
        this.type = type;
        this.name = name;
        this.specializer = specializer;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSpecializer() {
        return specializer;
    }

    public void setSpecializer(String specializer) {
        this.specializer = specializer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Attribute attribute = (Attribute) o;
        return Objects.equals(type, attribute.type) &&
                Objects.equals(name, attribute.name) &&
                Objects.equals(specializer, attribute.specializer);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, name, specializer);
    }

    @Override
    public String toString() {
        return type + ':' + name + ((specializer == null) ? "" : ":" + specializer);
    }
}
