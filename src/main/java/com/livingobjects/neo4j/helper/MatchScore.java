package com.livingobjects.neo4j.helper;

import com.google.common.base.MoreObjects;

import java.util.Objects;

public final class MatchScore implements Comparable<MatchScore> {

    private final int matchingAttributes;
    private final int matchingAttributeTypes;
    private final int notFoundAttributes;

    public MatchScore(int matchingAttributes, int matchingAttributeTypes, int notFoundAttributes) {
        this.matchingAttributes = matchingAttributes;
        this.matchingAttributeTypes = matchingAttributeTypes;
        this.notFoundAttributes = notFoundAttributes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MatchScore that = (MatchScore) o;
        return matchingAttributes == that.matchingAttributes &&
                matchingAttributeTypes == that.matchingAttributeTypes &&
                notFoundAttributes == that.notFoundAttributes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(matchingAttributes, matchingAttributeTypes, notFoundAttributes);
    }

    @Override
    public int compareTo(MatchScore o) {
        int compare = o.matchingAttributes - matchingAttributes;
        if (compare == 0) {
            compare = o.matchingAttributeTypes - matchingAttributeTypes;
            if (compare == 0) {
                compare = notFoundAttributes - o.notFoundAttributes;
            }
        }
        return compare;
    }

    public boolean matches() {
        return matchingAttributes > 0 || matchingAttributeTypes > 0;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("matchingAttributes", matchingAttributes)
                .add("matchingAttributeTypes", matchingAttributeTypes)
                .add("notFoundAttributes", notFoundAttributes)
                .toString();
    }
}