package com.livingobjects.neo4j.model.iwan;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Map;

public final class Relationship {

    public final String from;
    public final String to;
    public final String type;
    public final ImmutableMap<String, Object> attributes;

    public Relationship(@JsonProperty("from") String from,
                        @JsonProperty("to") String to,
                        @JsonProperty("type") String type,
                        @JsonProperty("attributes") Map<String, Object> attributes) {
        this.from = from;
        this.to = to;
        this.type = type;
        this.attributes = ImmutableMap.copyOf(attributes);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Relationship that = (Relationship) o;

        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        if (to != null ? !to.equals(that.to) : that.to != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
    }

    @Override
    public int hashCode() {
        int result = from != null ? from.hashCode() : 0;
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("from", from)
                .add("to", to)
                .add("type", type)
                .add("attributes", attributes)
                .toString();
    }
}
