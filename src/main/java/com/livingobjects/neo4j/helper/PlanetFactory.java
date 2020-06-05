package com.livingobjects.neo4j.helper;

import com.livingobjects.neo4j.loader.Scope;
import com.livingobjects.neo4j.model.iwan.Labels;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Optional;

import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.NAME;
import static com.livingobjects.neo4j.model.iwan.GraphModelConstants.SCOPE;

public class PlanetFactory {

    private final UniqueElementFactory delegate;

    public PlanetFactory(GraphDatabaseService graphdb) {
        delegate = new UniqueElementFactory(graphdb, Labels.PLANET, Optional.empty());
    }

    public UniqueEntity<Node> getOrCreate(String planetTemplate, Scope scope, Transaction tx) {
        String name = getPlanetName(planetTemplate, scope);
        UniqueEntity<Node> planet = delegate.getOrCreateWithOutcome(NAME, name, tx);
        planet.wasCreated(p -> p.setProperty(SCOPE, scope.tag));
        return planet;
    }

    public Node get(String planetTemplate, Scope scope, Transaction tx) {
        return get(planetTemplate, scope.id, tx);
    }

    public Node get(String planetTemplate, String scopeId, Transaction tx) {
        String name = getPlanetName(planetTemplate, scopeId);
        return name.contains(scopeId) ? delegate.getWithOutcome(NAME, name, tx) : null;
    }

    public String getPlanetName(String planetTemplate, Scope scope) {
        return getPlanetName(planetTemplate, scope.id);
    }

    public String getPlanetName(String planetTemplate, String scopeId) {
        return planetTemplate.replace(TemplatedPlanetFactory.PLACEHOLDER, scopeId);
    }

}
