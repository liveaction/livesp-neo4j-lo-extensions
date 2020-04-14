package com.livingobjects.neo4j.model.schema.update;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.schema.CounterNode;
import com.livingobjects.neo4j.model.schema.RealmPathSegment;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Set;

public final class AppendCounter extends SchemaUpdate {

    public final ImmutableSet<String> attributes;

    public final ImmutableList<RealmPathSegment> realmPath;

    public final CounterNode counter;

    public AppendCounter(@JsonProperty("schema") String schema,
                  @JsonProperty("realmTemplate") String realmTemplate,
                  @JsonProperty("attributes") Set<String> attributes,
                  @JsonProperty("realmPath") List<RealmPathSegment> realmPath,
                  @JsonProperty("counter") CounterNode counter) {
        super(schema, realmTemplate);
        this.attributes = ImmutableSet.copyOf(attributes);
        this.realmPath = ImmutableList.copyOf(realmPath);
        this.counter = counter;
    }

    @Override
    public <T> T visit(SchemaUpdateVisitor<T> visitor) {
        return visitor.appendCounter(schema, realmTemplate, attributes, realmPath, counter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        AppendCounter that = (AppendCounter) o;

        if (attributes != null ? !attributes.equals(that.attributes) : that.attributes != null) return false;
        if (realmPath != null ? !realmPath.equals(that.realmPath) : that.realmPath != null) return false;
        return counter != null ? counter.equals(that.counter) : that.counter == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (realmPath != null ? realmPath.hashCode() : 0);
        result = 31 * result + (counter != null ? counter.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("attributes", attributes)
                .add("realmPath", realmPath)
                .add("counter", counter)
                .toString();
    }
}
