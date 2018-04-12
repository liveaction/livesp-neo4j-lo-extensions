package com.livingobjects.neo4j.model.schema;

import com.google.common.collect.ImmutableList;
import com.livingobjects.neo4j.model.schema.planet.PlanetUpdate;
import com.livingobjects.neo4j.model.schema.update.SchemaUpdate;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.Nullable;
import java.util.List;

public class SchemaAndPlanetsUpdate {

    public final String version;

    public final ImmutableList<SchemaUpdate> schemaUpdates;

    public final ImmutableList<PlanetUpdate> planetUpdates;

    public SchemaAndPlanetsUpdate(@JsonProperty("version") @Nullable String version,
                                  @JsonProperty("schemaUpdates") List<SchemaUpdate> schemaUpdates,
                                  @JsonProperty("planetUpdates") List<PlanetUpdate> planetUpdates) {
        this.version = version;
        this.schemaUpdates = ImmutableList.copyOf(schemaUpdates);
        this.planetUpdates = ImmutableList.copyOf(planetUpdates);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaAndPlanetsUpdate that = (SchemaAndPlanetsUpdate) o;

        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        if (schemaUpdates != null ? !schemaUpdates.equals(that.schemaUpdates) : that.schemaUpdates != null)
            return false;
        return planetUpdates != null ? planetUpdates.equals(that.planetUpdates) : that.planetUpdates == null;
    }

    @Override
    public int hashCode() {
        int result = version != null ? version.hashCode() : 0;
        result = 31 * result + (schemaUpdates != null ? schemaUpdates.hashCode() : 0);
        result = 31 * result + (planetUpdates != null ? planetUpdates.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SchemaAndPlanetsUpdate{");
        sb.append("version='").append(version).append('\'');
        sb.append(", schemaUpdates=").append(schemaUpdates);
        sb.append(", planetUpdates=").append(planetUpdates);
        sb.append('}');
        return sb.toString();
    }
}
