package com.livingobjects.neo4j.model.export.query;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Objects;

public final class Pair<T, U> {
    public final T first;
    public final U second;

    public Pair(@JsonProperty("first") T first, @JsonProperty("second") U second) {
        this.first = first;
        this.second = second;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Pair other = (Pair) obj;
        return Objects.equals(first, other.first) && Objects.equals(second, other.second);
    }

    @Override
    public int hashCode() {
        int result = first != null ? first.hashCode() : 0;
        result = 31 * result + (second != null ? second.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "{ " + first + ", " + second + " }";
    }
}
