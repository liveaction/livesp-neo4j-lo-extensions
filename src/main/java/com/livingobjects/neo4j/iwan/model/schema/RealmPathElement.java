package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Set;

public final class RealmPathElement {

    public Planet planet;

    public Set<Counter> counters;

    public RealmPathElement() {
    }

    public RealmPathElement(
            Planet planet,
            Set<Counter> counters) {
        this.planet = planet;
        this.counters = counters;
    }

    public Planet getPlanet() {
        return planet;
    }

    public void setPlanet(Planet planet) {
        this.planet = planet;
    }

    public Set<Counter> getCounters() {
        return counters;
    }

    public void setCounters(Set<Counter> counters) {
        this.counters = counters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RealmPathElement that = (RealmPathElement) o;
        return Objects.equals(planet, that.planet) &&
                Objects.equals(counters, that.counters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planet, counters);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("planet", planet)
                .add("counters", counters)
                .toString();
    }
}
