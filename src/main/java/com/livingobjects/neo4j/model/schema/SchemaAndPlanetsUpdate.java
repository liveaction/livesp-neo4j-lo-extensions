package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.livingobjects.neo4j.model.schema.planet.PlanetUpdate;
import com.livingobjects.neo4j.model.schema.update.SchemaUpdate;

public class SchemaAndPlanetsUpdate {

    public final ImmutableList<SchemaUpdate> schemaUpdates;

    public final ImmutableList<PlanetUpdate> planetUpdates;

    public SchemaAndPlanetsUpdate(ImmutableList<SchemaUpdate> schemaUpdates, ImmutableList<PlanetUpdate> planetUpdates) {
        this.schemaUpdates = schemaUpdates;
        this.planetUpdates = planetUpdates;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaAndPlanetsUpdate that = (SchemaAndPlanetsUpdate) o;

        if (schemaUpdates != null ? !schemaUpdates.equals(that.schemaUpdates) : that.schemaUpdates != null)
            return false;
        return planetUpdates != null ? planetUpdates.equals(that.planetUpdates) : that.planetUpdates == null;
    }

    @Override
    public int hashCode() {
        int result = schemaUpdates != null ? schemaUpdates.hashCode() : 0;
        result = 31 * result + (planetUpdates != null ? planetUpdates.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("schemaUpdates", schemaUpdates)
                .add("planetUpdates", planetUpdates)
                .toString();
    }
}
