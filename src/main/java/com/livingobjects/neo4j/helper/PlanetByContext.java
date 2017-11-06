package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.model.exception.InsufficientContextException;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.stream.Collectors;

final class PlanetByContext {

    private final ImmutableMap<String, ImmutableMap<String, ImmutableSet<AttributeMatch>>> planets;

    PlanetByContext(Map<String, ImmutableSet<String>> planetAttributes) {

        ImmutableMap.Builder<String, ImmutableMap<String, ImmutableSet<AttributeMatch>>> planetAttributeMatchesBuilder = ImmutableMap.builder();
        for (Map.Entry<String, ImmutableSet<String>> planetAttributesEntry : planetAttributes.entrySet()) {
            String planet = planetAttributesEntry.getKey();
            Map<String, Set<AttributeMatch>> attributeMatchesByName = Maps.newHashMap();
            for (String attributeDefinition : planetAttributesEntry.getValue()) {
                AttributeMatch attributeMatch = AttributeMatch.parse(attributeDefinition);
                Set<AttributeMatch> attributeMatches = attributeMatchesByName.computeIfAbsent(attributeMatch.name, k -> Sets.newHashSet());
                attributeMatches.add(attributeMatch);
            }
            ImmutableMap.Builder<String, ImmutableSet<AttributeMatch>> mapBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Set<AttributeMatch>> entry : attributeMatchesByName.entrySet()) {
                mapBuilder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
            }
            planetAttributeMatchesBuilder.put(planet, mapBuilder.build());
        }
        this.planets = planetAttributeMatchesBuilder.build();
    }

    String bestMatchingContext(Set<String> elementContext) {

        SortedMap<MatchScore, Set<String>> matchingPlanetsByScore = Maps.newTreeMap();

        for (Map.Entry<String, ImmutableMap<String, ImmutableSet<AttributeMatch>>> entry : planets.entrySet()) {
            String planet = entry.getKey();
            ImmutableMap<String, ImmutableSet<AttributeMatch>> planetAttributes = entry.getValue();
            MatchScore score = computeScore(planetAttributes, elementContext);
            Set<String> matches = matchingPlanetsByScore.computeIfAbsent(score, k -> Sets.newHashSet());
            matches.add(planet);
        }

        return getBestMacthesIfNoConflicts(matchingPlanetsByScore);
    }

    private MatchScore computeScore(ImmutableMap<String, ImmutableSet<AttributeMatch>> planetAttributes, Set<String> elementContext) {
        int matchingAttributes = 0;
        int matchingAttributeTypes = 0;
        for (String contextAttribute : elementContext) {
            AttributeMatch.Value attributeMatchValue = AttributeMatch.value(contextAttribute);
            ImmutableSet<AttributeMatch> matches = planetAttributes.get(attributeMatchValue.name);
            if (matches != null) {
                boolean matchValue = false;
                boolean matchType = false;
                for (AttributeMatch match : matches) {
                    if (match.matchValue(attributeMatchValue.value)) {
                        if (match.matchAll(attributeMatchValue.value)) {
                            matchType = true;
                        } else {
                            matchValue = true;
                            break;
                        }
                    }
                }
                if (matchValue) {
                    matchingAttributes++;
                } else {
                    if (matchType) matchingAttributeTypes++;
                }
            }
        }
        int notFoundAttributes = planetAttributes.keySet().size() - (matchingAttributes + matchingAttributeTypes);
        return new MatchScore(matchingAttributes, matchingAttributeTypes, notFoundAttributes);
    }

    private String getBestMacthesIfNoConflicts(SortedMap<MatchScore, Set<String>> matchingPlanetsByScore) {
        String bestMatch = null;
        for (Map.Entry<MatchScore, Set<String>> matchEntry : matchingPlanetsByScore.entrySet()) {
            if (matchEntry.getKey().matches()) {
                if (matchEntry.getValue().size() == 1) {
                    bestMatch = Iterables.getOnlyElement(matchEntry.getValue());
                    break;
                } else {
                    throw new InsufficientContextException("Multiple PlanetTemplate eligible but no sufficient context to choose certainty !", distinctAttributes(matchEntry.getValue()));
                }
            }
        }
        if (bestMatch == null) {
            throw new InsufficientContextException("No PlanetTemplate eligible for this context !", ImmutableSet.copyOf(planets.values().stream()
                    .flatMap(stringImmutableSetImmutableMap -> stringImmutableSetImmutableMap.values().stream()
                            .flatMap(v -> v.stream().map(AttributeMatch::toString)))
                    .collect(Collectors.toList())));
        }
        return bestMatch;
    }

    ImmutableSet<String> distinctAttributes(Set<String> conflictingPlanets) {
        if (conflictingPlanets.size() > 1) {
            Map<String, Integer> attributesByCount = Maps.newHashMap();
            for (String conflictingPlanet : conflictingPlanets) {
                ImmutableMap<String, ImmutableSet<AttributeMatch>> attributes = planets.get(conflictingPlanet);
                if (attributes == null) {
                    throw new IllegalArgumentException("Planet not found in this context '" + conflictingPlanet + "'");
                }
                for (ImmutableSet<AttributeMatch> attributeMatches : attributes.values()) {
                    for (AttributeMatch attributeMatch : attributeMatches) {
                        String attribute = attributeMatch.toString();
                        Integer hits = attributesByCount.getOrDefault(attribute, 0);
                        attributesByCount.put(attribute, hits + 1);
                    }
                }
            }
            return ImmutableSet.copyOf(attributesByCount.entrySet().stream()
                    .filter(e -> e.getValue() < conflictingPlanets.size())
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet()));
        } else {
            return ImmutableSet.of();
        }
    }

    private static abstract class AttributeMatch {

        public final String name;

        abstract boolean matchAll(String testValue);

        abstract boolean matchValue(String testValue);

        public static AttributeMatch parse(String attributeDefinition) {
                return value(attributeDefinition);
        }

        public static AttributeMatch.Value value(String attributeDefinition) {
            if (attributeDefinition.endsWith(":*")) {
                throw new IllegalArgumentException("Wrong attribute definition because end with ':*' : '" + attributeDefinition + "'");
            }
            String[] split = attributeDefinition.split(":", 2);
            if (split.length != 2) {
                throw new IllegalArgumentException("Wrong attribute '" + attributeDefinition + "'");
            }
            return new Value(split[0], split[1]);
        }

        private AttributeMatch(String name) {
            this.name = name;
        }

        static class Value extends AttributeMatch {
            public final String value;

            Value(String name, String value) {
                super(name);
                this.value = value;
            }

            @Override
            public String toString() {
                return name + ":" + value;
            }

            @Override
            boolean matchAll(String testValue) {
                return false;
            }

            @Override
            boolean matchValue(String testValue) {
                return value.equals(testValue);
            }
        }

    }

}