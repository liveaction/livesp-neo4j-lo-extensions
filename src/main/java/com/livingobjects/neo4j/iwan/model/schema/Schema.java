package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.base.MoreObjects;
import com.livingobjects.neo4j.iwan.model.schema.model.SchemaVersion;

import java.util.Objects;
import java.util.Set;

public final class Schema {

    public SchemaVersion version;

    public String customerId;

    public String scope;

    public Set<Attribute> attributes;

    public Set<Planet> planets;

    public Set<RealmTemplate> realms;

    public Schema() {
    }

    public Schema(
            SchemaVersion version,
            String customerId,
            String scope,
            Set<Attribute> attributes,
            Set<Planet> planets,
            Set<RealmTemplate> realms) {
        this.version = version;
        this.customerId = customerId;
        this.scope = scope;
        this.attributes = attributes;
        this.planets = planets;
        this.realms = realms;
    }

    public SchemaVersion getVersion() {
        return version;
    }

    public void setVersion(SchemaVersion version) {
        this.version = version;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getScope() {
        return scope;
    }

    public void setScope(String scope) {
        this.scope = scope;
    }

    public Set<Attribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(Set<Attribute> attributes) {
        this.attributes = attributes;
    }

    public Set<Planet> getPlanets() {
        return planets;
    }

    public void setPlanets(Set<Planet> planets) {
        this.planets = planets;
    }

    public Set<RealmTemplate> getRealms() {
        return realms;
    }

    public void setRealms(Set<RealmTemplate> realms) {
        this.realms = realms;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return Objects.equals(version, schema.version) &&
                Objects.equals(customerId, schema.customerId) &&
                Objects.equals(scope, schema.scope) &&
                Objects.equals(attributes, schema.attributes) &&
                Objects.equals(planets, schema.planets) &&
                Objects.equals(realms, schema.realms);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, customerId, scope, attributes, planets, realms);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("version", version)
                .add("customerId", customerId)
                .add("scope", scope)
                .add("attributes", attributes)
                .add("planets", planets)
                .add("realms", realms)
                .toString();
    }
}
