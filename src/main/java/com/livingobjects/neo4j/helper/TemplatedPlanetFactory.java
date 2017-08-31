package com.livingobjects.neo4j.helper;

import com.google.common.collect.ImmutableMap;
import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.Labels;
import com.livingobjects.neo4j.model.iwan.RelationshipTypes;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.*;

public class TemplatedPlanetFactory {
    private static final String PLACEHOLDER = "{:scopeId}";

    private final UniqueElementFactory planetFactory;

    private final ImmutableMap<String, String> planetNameTemplateCache;

    public TemplatedPlanetFactory(GraphDatabaseService graphDb) {
        this.planetFactory = UniqueElementFactory.planetFactory(graphDb);
        this.planetNameTemplateCache = loadPlanetTemplateName(graphDb);
    }

    public UniqueEntity<Node> createOrUpdatePlanet(Scope solidScope, String keyType) {
        String planetTemplateName = planetNameTemplateCache.get(keyType);
        if (planetTemplateName == null) {
            throw new IllegalStateException(String.format("Unable to instantiate planet for '%s'. No PlanetTemplate found.", keyType));
        }
        String planetName = planetTemplateName.replace(PLACEHOLDER, solidScope.id);
        UniqueEntity<Node> planet = planetFactory.getOrCreateWithOutcome(NAME, planetName);
        planet.wasCreated(p -> p.setProperty(SCOPE, solidScope.tag));
        return planet;
    }

    private ImmutableMap<String, String> loadPlanetTemplateName(GraphDatabaseService graphDb) {
        ImmutableMap.Builder<String, String> tplCacheBuilder = ImmutableMap.builder();
        graphDb.findNodes(Labels.PLANET_TEMPLATE).forEachRemaining(pltNode -> {
            String name = pltNode.getProperty(NAME).toString();
            String keyType;
            for (Relationship aRelation : pltNode.getRelationships(Direction.OUTGOING, RelationshipTypes.ATTRIBUTE)) {
                Node attNode = aRelation.getEndNode();
                if (!attNode.hasLabel(Labels.ATTRIBUTE)) continue;
                String type = attNode.getProperty(_TYPE).toString();
                if (KEY_TYPES.contains(type)) {
                    keyType = type + KEYTYPE_SEPARATOR + attNode.getProperty(NAME).toString();
                    tplCacheBuilder.put(keyType, name);
                    break;
                }
            }
        });
        return tplCacheBuilder.build();
    }
}
