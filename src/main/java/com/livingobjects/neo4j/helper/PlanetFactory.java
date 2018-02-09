package com.livingobjects.neo4j.helper;

import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.Labels;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.IwanModelConstants.SCOPE;

public class PlanetFactory {

    private final UniqueElementFactory delegate;

    public PlanetFactory(GraphDatabaseService graphdb) {
        delegate = new UniqueElementFactory(graphdb, Labels.PLANET, Optional.empty());
    }

    public UniqueEntity<Node> getOrCreate(String planetTemplate, Scope scope) {
        String name = getPlanetName(planetTemplate, scope);
        UniqueEntity<Node> planet = delegate.getOrCreateWithOutcome(NAME, name);
        planet.wasCreated(p -> p.setProperty(SCOPE, scope.tag));
        return planet;
    }

    public String getPlanetName(String planetTemplate, Scope scope) {
        return planetTemplate.replace(TemplatedPlanetFactory.PLACEHOLDER, scope.id);
    }

}
