package com.livingobjects.neo4j.model.schema;

import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonProperty;

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
