package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.exception.InsufficientContextException;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

public class TemplatedPlanetFactory {
    private static final String PLACEHOLDER = "{:scopeId}";

    private final UniqueElementFactory planetFactory;

    private final ImmutableMap<String, PlanetByContext> planetNameTemplateCache;

    public TemplatedPlanetFactory(GraphDatabaseService graphDb) {
        this.planetFactory = UniqueElementFactory.planetFactory(graphDb);
        this.planetNameTemplateCache = loadPlanetTemplateName(graphDb);
    }

    public UniqueEntity<Node> localizePlanetForElement(Scope solidScope, Node element) {
        String keyType = element.getProperty(_TYPE).toString();
        PlanetByContext planetByContext = planetNameTemplateCache.get(keyType);
        if (planetByContext == null) {
            throw new IllegalStateException(String.format("Unable to instantiate planet for '%s'. No PlanetTemplate found.", keyType));
        }
        ImmutableSet<String> elementContext = ImmutableSet.copyOf(element.getAllProperties().entrySet().stream()
                .map(e -> e.getKey() + ':' + e.getValue())
                .collect(Collectors.toSet()));

        String planetTemplateName;
        try {
            planetTemplateName = planetByContext.bestMatchingContext(elementContext);

        } catch (InsufficientContextException e) {
            Set<String> specificContext = findMoreSpecificContext(element).entrySet().stream()
                    .map(en -> en.getKey() + ':' + en.getValue())
                    .collect(Collectors.toSet());
            planetTemplateName = planetByContext.bestMatchingContext(specificContext);
        }

        String planetName = planetTemplateName.replace(PLACEHOLDER, solidScope.id);
        UniqueEntity<Node> planet = planetFactory.getOrCreateWithOutcome(NAME, planetName);
        planet.wasCreated(p -> p.setProperty(SCOPE, solidScope.tag));
        return planet;
    }

    private ImmutableMap<String, String> findMoreSpecificContext(Node element) {
        Map<String, Object> properties = element.getAllProperties();
        Map<String, String> specificContext = Maps.newHashMapWithExpectedSize(properties.size());
        for (Relationship rel : element.getRelationships(RelationshipTypes.CONNECT, Direction.OUTGOING)) {
            Node parent = rel.getEndNode();
            ImmutableMap<String, String> context = findMoreSpecificContext(parent);
            specificContext.putAll(context);
        }
        specificContext.putAll(Maps.transformValues(properties, Object::toString));

        return ImmutableMap.copyOf(specificContext);
    }

    private ImmutableMap<String, PlanetByContext> loadPlanetTemplateName(GraphDatabaseService graphDb) {
        ImmutableSetMultimap.Builder<String, Entry<String, ImmutableSet<String>>> tplCacheBuilder = ImmutableSetMultimap.builder();
        graphDb.findNodes(Labels.PLANET_TEMPLATE).forEachRemaining(pltNode -> {
            String keyType = null;
            ImmutableSet.Builder<String> contexts = ImmutableSet.builder();
            for (Relationship aRelation : pltNode.getRelationships(Direction.OUTGOING, RelationshipTypes.ATTRIBUTE)) {
                Node attNode = aRelation.getEndNode();
                if (!attNode.hasLabel(Labels.ATTRIBUTE)) continue;
                String type = attNode.getProperty(_TYPE).toString();
                String context = type + KEYTYPE_SEPARATOR + attNode.getProperty(NAME).toString();
                if (KEY_TYPES.contains(type)) keyType = context;
                contexts.add(context);
            }
            assert keyType != null;
            tplCacheBuilder.put(keyType, Maps.immutableEntry(pltNode.getProperty(NAME).toString(), contexts.build()));
        });

        ImmutableMap.Builder<String, PlanetByContext> result = ImmutableMap.builder();
        tplCacheBuilder.build().asMap().forEach((keyType, planets) ->
                result.put(keyType, new PlanetByContext(planets)));

        return result.build();
    }

    private static final class PlanetByContext {
        private final ImmutableSet<Entry<String, ImmutableSet<String>>> planets;

        PlanetByContext(Collection<Entry<String, ImmutableSet<String>>> planets) {
            this.planets = ImmutableSet.copyOf(planets);
        }

        String bestMatchingContext(Set<String> elementContext) {
            if (planets.size() == 1) {
                return Iterables.getOnlyElement(planets).getKey();
            }
            String planetTemplateName = null;
            int bestRank = 0;
            for (Entry<String, ImmutableSet<String>> planet : planets) {
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
    }
}
