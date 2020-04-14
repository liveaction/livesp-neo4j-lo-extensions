package com.livingobjects.neo4j.model.schema;

import com.google.common.collect.ImmutableList;
import com.livingobjects.neo4j.model.schema.planet.PlanetUpdate;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public final class SchemaAndPlanets {

    public final Schema schema;

    public final ImmutableList<PlanetUpdate> planetMigrations;

    public SchemaAndPlanets(@JsonProperty("schema") Schema schema,
                            @JsonProperty("planetMigrations") List<PlanetUpdate> planetMigrations) {
        this.schema = schema;
        this.planetMigrations = ImmutableList.copyOf(planetMigrations);
    }
}
