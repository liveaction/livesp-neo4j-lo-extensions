package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.exception.InsufficientContextException;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

final class PlanetByContext {

    private final ImmutableSet<Map.Entry<String, ImmutableSet<String>>> planets;

    PlanetByContext(Collection<Map.Entry<String, ImmutableSet<String>>> planets) {
        this.planets = ImmutableSet.copyOf(planets);
    }

    String bestMatchingContext(Set<String> elementContext) {

        SortedSet<Comparable> comparables = Sets.newTreeSet();


        String planetTemplateName = null;
        int highestIntersectionCount = 0;
        int lowestDifferenceCount = Integer.MAX_VALUE;
        for (Map.Entry<String, ImmutableSet<String>> planet : planets) {
            int intersection = Sets.intersection(planet.getValue(), elementContext).size();
            int difference = Sets.difference(planet.getValue(), elementContext).size();
            if (intersection > highestIntersectionCount || difference < lowestDifferenceCount) {
                highestIntersectionCount = intersection;
                lowestDifferenceCount = difference;
                planetTemplateName = planet.getKey();
            }
        }

        if (planetTemplateName == null) {
            throw new IllegalStateException("No PlanetTemplate eligible for this context !");
        }
        if (highestIntersectionCount == 0 && planets.size() > 1) {
            //throw new InsufficientContextException("Multiple PlanetTemplate eligible but no sufficient context to choose certainty !");
            throw new InsufficientContextException("No planet found");
        }

        return planetTemplateName;
    }

    ImmutableSet<String> distinctAttributes() {
        if (planets.size() > 1) {
            Map<String, Integer> attributesByCount = Maps.newHashMap();
            for (Map.Entry<String, ImmutableSet<String>> planet : planets) {
                for (String attribute : planet.getValue()) {
                    Integer hits = attributesByCount.getOrDefault(attribute, 0);
                    attributesByCount.put(attribute, hits + 1);
                }
            }
            return ImmutableSet.copyOf(attributesByCount.entrySet().stream()
                    .filter(e -> e.getValue() < planets.size())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet()));
        } else {
            return ImmutableSet.of();
        }
    }

    static class MatchScore implements Comparable<MatchScore> {

        private final int matchingAttributes;
        private final int matchingAttributeTypes;
        private final int conflictingAttributes;
        private final int notFoundAttributes;

        public MatchScore(int matchingAttributes, int matchingAttributeTypes, int conflictingAttributes, int notFoundAttributes) {
            this.matchingAttributes = matchingAttributes;
            this.matchingAttributeTypes = matchingAttributeTypes;
            this.conflictingAttributes = conflictingAttributes;
            this.notFoundAttributes = notFoundAttributes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MatchScore that = (MatchScore) o;
            return matchingAttributes == that.matchingAttributes &&
                    matchingAttributeTypes == that.matchingAttributeTypes &&
                    conflictingAttributes == that.conflictingAttributes &&
                    notFoundAttributes == that.notFoundAttributes;
        }

        @Override
        public int hashCode() {
            return Objects.hash(matchingAttributes, matchingAttributeTypes, conflictingAttributes, notFoundAttributes);
        }

        @Override
        public int compareTo(MatchScore o) {
            int compare = matchingAttributes - o.matchingAttributes;
            if (compare == 0) {
                compare = matchingAttributeTypes - o.matchingAttributeTypes;
                if (compare == 0) {
                    compare = o.conflictingAttributes - conflictingAttributes;
                    if (compare == 0) {
                        compare = o.notFoundAttributes - notFoundAttributes;
                    }
                }
            }
            return compare;
        }

    }

}