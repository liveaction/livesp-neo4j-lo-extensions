package com.livingobjects.neo4j.model.schema;

import com.google.common.collect.ImmutableList;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;

public final class SchemaAndPlanets {

    public final Schema schema;

    public final ImmutableList<PlanetMigration> planets;

    public SchemaAndPlanets(@JsonProperty("schema") Schema schema,
                            @JsonProperty("planets") List<PlanetMigration> planets) {
        this.schema = schema;
        this.planets = ImmutableList.copyOf(planets);
    }
}
