package com.livingobjects.neo4j.model.schema.managed;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.livingobjects.neo4j.model.schema.RealmNode;
import com.livingobjects.neo4j.model.schema.Schema;

public final class ManagedSchema {

    public final ImmutableMap<String, RealmNode> realms;

    public final CountersDefinition counters;

    public ManagedSchema(ImmutableMap<String, RealmNode> realms, CountersDefinition counters) {
        this.realms = realms;
        this.counters = counters;
    }

    public static ManagedSchema fullyManaged(Schema schema) {
        return new ManagedSchema(schema.realms, CountersDefinition.fullyManaged(schema.counters));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ManagedSchema that = (ManagedSchema) o;

        if (realms != null ? !realms.equals(that.realms) : that.realms != null) return false;
        return counters != null ? counters.equals(that.counters) : that.counters == null;
    }

    @Override
    public int hashCode() {
        int result = realms != null ? realms.hashCode() : 0;
        result = 31 * result + (counters != null ? counters.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("realms", realms)
                .add("counters", counters)
                .toString();
    }
}
