package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.exception.InsufficientContextException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

final class PlanetByContext {

    private final ImmutableSet<Map.Entry<String, ImmutableSet<String>>> planets;

    PlanetByContext(Collection<Map.Entry<String, ImmutableSet<String>>> planets) {
        this.planets = ImmutableSet.copyOf(planets);
    }

    String bestMatchingContext(Set<String> elementContext) {
        if (planets.size() == 1) {
            return Iterables.getOnlyElement(planets).getKey();
        }
        String planetTemplateName = null;
        int bestRank = 0;
        for (Map.Entry<String, ImmutableSet<String>> planet : planets) {
            int rank = Sets.intersection(planet.getValue(), elementContext).size();
            if (rank >= bestRank) {
                bestRank = rank;
                planetTemplateName = planet.getKey();
            }
        }

        if (planetTemplateName == null) {
            throw new IllegalStateException("No PlanetTemplate eligible for this context !");
        }
        if (bestRank == 0 && planets.size() > 1) {
            throw new InsufficientContextException("Multiple PlanetTemplate eligible but no sufficient context to choose certainty !");
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
}