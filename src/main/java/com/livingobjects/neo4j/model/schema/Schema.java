package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public final class Schema {

    public final String id;

    public final String version;

    public final ImmutableMap<String, MemdexPathNode> realms;

    public final ImmutableMap<String, PlanetNode> planets;

    public final ImmutableMap<String, CounterNode> counters;

    public Schema(@JsonProperty("id") String id,
                  @JsonProperty("version") String version,
                  @JsonProperty("realms") Map<String, MemdexPathNode> realms,
                  @JsonProperty("planets") Map<String, PlanetNode> planets,
                  @JsonProperty("counters") Map<String, CounterNode> counters) {
        this.id = id;
        this.version = version;
        this.realms = ImmutableMap.copyOf(realms);
        this.planets = ImmutableMap.copyOf(planets);
        this.counters = ImmutableMap.copyOf(counters);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Schema schema = (Schema) o;

        if (id != null ? !id.equals(schema.id) : schema.id != null) return false;
        if (version != null ? !version.equals(schema.version) : schema.version != null) return false;
        if (realms != null ? !realms.equals(schema.realms) : schema.realms != null) return false;
        if (planets != null ? !planets.equals(schema.planets) : schema.planets != null) return false;
        return counters != null ? counters.equals(schema.counters) : schema.counters == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (realms != null ? realms.hashCode() : 0);
        result = 31 * result + (planets != null ? planets.hashCode() : 0);
        result = 31 * result + (counters != null ? counters.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", id)
                .add("version", version)
                .add("realms", realms)
                .add("planets", planets)
                .add("counters", counters)
                .toString();
    }
}
