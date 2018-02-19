package com.livingobjects.neo4j.model.schema;

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public final class Schema {

    public final String id;

    public final String version;

    public final ImmutableMap<String, RealmNode> realms;

    public final ImmutableMap<String, CounterNode> counters;

    public Schema(@JsonProperty("id") String id,
                   @JsonProperty("version") String version,
                   @JsonProperty("realms") Map<String, RealmNode> realms,
                   @JsonProperty("counters") Map<String, CounterNode> counters) {
        this.id = id;
        this.version = version;
        this.realms = ImmutableMap.copyOf(realms);
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
        return counters != null ? counters.equals(schema.counters) : schema.counters == null;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (realms != null ? realms.hashCode() : 0);
        result = 31 * result + (counters != null ? counters.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Schema{");
        sb.append("id='").append(id).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", realms=").append(realms);
        sb.append(", counters=").append(counters);
        sb.append('}');
        return sb.toString();
    }
}
