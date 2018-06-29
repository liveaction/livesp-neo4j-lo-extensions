package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.KEYTYPE_SEPARATOR;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.KEY_TYPES;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants._TYPE;

public class TemplatedPlanetFactory {
    public static final String PLACEHOLDER = "{:scopeId}";

    private final PlanetFactory planetFactory;

    private final ImmutableMap<String, PlanetByContext> planetNameTemplateCache;

    public TemplatedPlanetFactory(GraphDatabaseService graphDb) {
        this.planetFactory = new PlanetFactory(graphDb);
        this.planetNameTemplateCache = loadPlanetTemplateName(graphDb);
    }

    public UniqueEntity<Node> localizePlanetForElement(Scope solidScope, Node element) {
        String keyType = element.getProperty(_TYPE).toString();
        PlanetByContext planetByContext = planetNameTemplateCache.get(keyType);
        if (planetByContext == null) {
            throw new IllegalStateException(String.format("Unable to instantiate planet for '%s'. No PlanetTemplate found.", keyType));
        }
        String planetTemplateName = localizePlanetForElement(element, planetByContext);

        return planetFactory.getOrCreate(planetTemplateName, solidScope);
    }

    public static String localizePlanetForElement(Node element, PlanetByContext planetByContext) {
        String keyType = element.getProperty(_TYPE).toString();

        String planetTemplateName;
        Set<String> specificContext = Sets.union(findMoreSpecificContext(element).entrySet().stream()
                .filter(en -> !en.getKey().startsWith("_"))
                .map(en -> en.getKey() + ':' + en.getValue())
                .collect(Collectors.toSet()), ImmutableSet.of(keyType));
        try {
            planetTemplateName = planetByContext.bestMatchingContext(specificContext);
        } catch (InsufficientContextException ignored) {
            throw new IllegalStateException(String.format("Unable to create '%s'. Missing attribute to determine context : '%s'. Line is ignored.", keyType, ignored.missingAttributesToChoose));
        }
        return planetTemplateName;
    }

    private static ImmutableMap<String, String> findMoreSpecificContext(Node element) {
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
        Map<String, Map<String, ImmutableSet<String>>> tplCacheBuilder = Maps.newHashMap();
        graphDb.findNodes(Labels.PLANET_TEMPLATE).forEachRemaining(pltNode -> {
            String keyType = null;
            ImmutableSet.Builder<String> contexts = ImmutableSet.builder();
            for (Relationship aRelation : pltNode.getRelationships(RelationshipTypes.ATTRIBUTE, Direction.OUTGOING)) {
                Node attNode = aRelation.getEndNode();
                if (!attNode.hasLabel(Labels.ATTRIBUTE)) continue;
                String type = attNode.getProperty(_TYPE).toString();
                String context = type + KEYTYPE_SEPARATOR + attNode.getProperty(NAME).toString();
                if (KEY_TYPES.contains(type)) keyType = context;
                contexts.add(context);
            }
            if (keyType == null) {
                String planetName = pltNode.getProperty(NAME).toString();
                throw new IllegalStateException("Schema cannot be loaded : the planet '" + planetName + "' does not have a valid keyAttribute");
            }
            Map<String, ImmutableSet<String>> attributesByPlanet = tplCacheBuilder.computeIfAbsent(keyType, k -> Maps.newHashMap());
            attributesByPlanet.put(pltNode.getProperty(NAME).toString(), contexts.build());
        });

        ImmutableMap.Builder<String, PlanetByContext> result = ImmutableMap.builder();
        tplCacheBuilder.forEach((keyType, planets) ->
                result.put(keyType, new PlanetByContext(planets)));

        return result.build();
    }

}
