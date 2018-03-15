package com.livingobjects.neo4j.model.schema.update;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.livingobjects.neo4j.model.schema.RealmPathSegment;

import java.util.List;

public final class DeleteCounter extends SchemaUpdate {

    public final ImmutableList<RealmPathSegment> realmPath;

    public final String counter;

    DeleteCounter(String schema,
                  String realmTemplate,
                  List<RealmPathSegment> realmPath,
                  String counter) {
        super(schema, realmTemplate);
        this.realmPath = ImmutableList.copyOf(realmPath);
        this.counter = counter;
    }

    @Override
    public <T> T visit(SchemaUpdateVisitor<T> visitor) {
        return visitor.deleteCounter(schema, realmTemplate, realmPath, counter);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        DeleteCounter that = (DeleteCounter) o;

        if (realmPath != null ? !realmPath.equals(that.realmPath) : that.realmPath != null) return false;
        return counter != null ? counter.equals(that.counter) : that.counter == null;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + (realmPath != null ? realmPath.hashCode() : 0);
        result = 31 * result + (counter != null ? counter.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("realmPath", realmPath)
                .add("counter", counter)
                .toString();
    }
}
