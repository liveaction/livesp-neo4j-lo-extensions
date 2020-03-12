package com.livingobjects.neo4j.model.schema.planet;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public final class PlanetUpdate {
    public final PlanetNode oldPlanet;
    public final PlanetUpdateStatus planetUpdateStatus;
    public final ImmutableSet<PlanetNode> newPlanets;

    public PlanetUpdate(@JsonProperty("oldPlanet") PlanetNode oldPlanet,
                        @JsonProperty("planetUpdateStatus") PlanetUpdateStatus planetUpdateStatus,
                        @JsonProperty("newPlanets") Set<PlanetNode> newPlanets) {
        this.oldPlanet = oldPlanet;
        this.planetUpdateStatus = planetUpdateStatus;
        this.newPlanets = ImmutableSet.copyOf(newPlanets);
    }
}
