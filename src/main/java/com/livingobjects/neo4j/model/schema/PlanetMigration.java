package com.livingobjects.neo4j.model.schema;

import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Set;

public final class PlanetMigration {
    public final ImmutableSet<PlanetNode> oldPlanets;
    public final ImmutableSet<PlanetNode> newPlanets;

    public PlanetMigration(@JsonProperty("oldPlanets") Set<PlanetNode> oldPlanets,
                           @JsonProperty("newPlanets") Set<PlanetNode> newPlanets) {
        this.oldPlanets = ImmutableSet.copyOf(oldPlanets);
        this.newPlanets = ImmutableSet.copyOf(newPlanets);
    }
}
