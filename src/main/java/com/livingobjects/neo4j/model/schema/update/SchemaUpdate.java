package com.livingobjects.neo4j.model.schema.update;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.model.schema.CounterNode;
import com.livingobjects.neo4j.model.schema.RealmPathSegment;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = AppendCounter.class, name = "appendCounter"),
        @JsonSubTypes.Type(value = DeleteCounter.class, name = "deleteCounter"),
})
public abstract class SchemaUpdate {

    public final String schema;

    public final String realmTemplate;

    public abstract <T> T visit(SchemaUpdateVisitor<T> visitor);

    SchemaUpdate(String schema, String realmTemplate) {
        this.schema = schema;
        this.realmTemplate = realmTemplate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaUpdate that = (SchemaUpdate) o;

        if (schema != null ? !schema.equals(that.schema) : that.schema != null) return false;
        return realmTemplate != null ? realmTemplate.equals(that.realmTemplate) : that.realmTemplate == null;
    }

    @Override
    public int hashCode() {
        int result = schema != null ? schema.hashCode() : 0;
        result = 31 * result + (realmTemplate != null ? realmTemplate.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("schema", schema)
                .add("realmTemplate", realmTemplate)
                .toString();
    }

    public interface SchemaUpdateVisitor<T> {

        T appendCounter(String schema,
                        String realmTemplate,
                        ImmutableSet<String> attributes,
                        ImmutableList<RealmPathSegment> realmPath,
                        CounterNode counter);

        T deleteCounter(String schema,
                        String realmTemplate,
                        ImmutableList<RealmPathSegment> realmPath,
                        String counter);

    }

}
