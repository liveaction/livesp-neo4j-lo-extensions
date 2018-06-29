package com.livingobjects.neo4j.model.schema.managed;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.schema.CounterNode;

import java.util.Map;
import java.util.Set;

public final class CountersDefinition {

    public final ImmutableMap<String, CounterNode> counters;

    private final ImmutableSet<String> managedCounters;

    private CountersDefinition(ImmutableMap<String, CounterNode> counters, ImmutableSet<String> managedCounters) {
        this.counters = counters;
        this.managedCounters = managedCounters;
    }

    public static CountersDefinition.Builder builder() {
        return new Builder();
    }

    static CountersDefinition fullyManaged(ImmutableMap<String, CounterNode> counters) {
        return new CountersDefinition(counters, ImmutableSet.copyOf(counters.keySet()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CountersDefinition that = (CountersDefinition) o;

        if (counters != null ? !counters.equals(that.counters) : that.counters != null) return false;
        return managedCounters != null ? managedCounters.equals(that.managedCounters) : that.managedCounters == null;
    }

    @Override
    public int hashCode() {
        int result = counters != null ? counters.hashCode() : 0;
        result = 31 * result + (managedCounters != null ? managedCounters.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("counters", counters)
                .add("managedCounters", managedCounters)
                .toString();
    }

    public boolean isManaged(String counter) {
        return managedCounters.contains(counter);
    }

    public CounterNode get(String counter) {
        return counters.get(counter);
    }

    public static class Builder {

        private final Map<String, CounterNode> counters = Maps.newHashMap();
        private final Set<String> managedCounters = Sets.newHashSet();

        public Builder addFullyManaged(Map<String, CounterNode> counterNodes) {
            counters.putAll(counterNodes);
            managedCounters.addAll(counterNodes.keySet());
            return this;
        }

        public Builder add(String id, CounterNode counterNode, boolean managed) {
            counters.put(id, counterNode);
            if (managed) {
                managedCounters.add(id);
            }
            return this;
        }

        public CountersDefinition build() {
            return new CountersDefinition(ImmutableMap.copyOf(counters), ImmutableSet.copyOf(managedCounters));
        }

    }

}
