package com.livingobjects.neo4j.model.schema;

import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Set;

public final class RealmNode {
    public final String name;
    public final ImmutableSet<String> attributes;
    public final MemdexPathNode memdexPath;

    public RealmNode(@JsonProperty("topologyRealm") String name,
                     @JsonProperty("attributes") Set<String> attributes,
                     @JsonProperty("memdexPath") MemdexPathNode memdexPath) {
        this.name = name;
        this.attributes = ImmutableSet.copyOf(attributes);
        this.memdexPath = memdexPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RealmNode realmNode = (RealmNode) o;

        if (name != null ? !name.equals(realmNode.name) : realmNode.name != null)
            return false;
        if (attributes != null ? !attributes.equals(realmNode.attributes) : realmNode.attributes != null) return false;
        return memdexPath != null ? memdexPath.equals(realmNode.memdexPath) : realmNode.memdexPath == null;
    }

    @Override
    public int hashCode() {
        int result = name != null ? name.hashCode() : 0;
        result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
        result = 31 * result + (memdexPath != null ? memdexPath.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RealmNode{");
        sb.append("topologyRealm='").append(name).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append(", memdexPath=").append(memdexPath);
        sb.append('}');
        return sb.toString();
    }
}
