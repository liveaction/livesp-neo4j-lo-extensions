package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

public final class SchemaRealmPath {

    public final String realm;

    public final MemdexPathNode realmPath;

    public final ImmutableMap<String, CounterNode> counters;

    public SchemaRealmPath(String realm, MemdexPathNode realmPath, ImmutableMap<String, CounterNode> counters) {
        this.realm = realm;
        this.realmPath = realmPath;
        this.counters = counters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaRealmPath that = (SchemaRealmPath) o;

        if (realm != null ? !realm.equals(that.realm) : that.realm != null) return false;
        if (realmPath != null ? !realmPath.equals(that.realmPath) : that.realmPath != null) return false;
        return counters != null ? counters.equals(that.counters) : that.counters == null;
    }

    @Override
    public int hashCode() {
        int result = realm != null ? realm.hashCode() : 0;
        result = 31 * result + (realmPath != null ? realmPath.hashCode() : 0);
        result = 31 * result + (counters != null ? counters.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("realm", realm)
                .add("realmPath", realmPath)
                .add("counters", counters)
                .toString();
    }
}
