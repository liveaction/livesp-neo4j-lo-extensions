package com.livingobjects.neo4j.model.iwan;

import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

public final class RelationshipStatus {

    public final String type;
    public final String from;
    public final String to;
    public final boolean imported;
    public final String message;

    public RelationshipStatus(String type,
                              String from,
                              String to,
                              boolean imported,
                              @Nullable String message) {
        this.type = type;
        this.from = from;
        this.to = to;
        this.imported = imported;
        this.message = message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RelationshipStatus that = (RelationshipStatus) o;

        if (imported != that.imported) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        if (from != null ? !from.equals(that.from) : that.from != null) return false;
        if (to != null ? !to.equals(that.to) : that.to != null) return false;
        return message != null ? message.equals(that.message) : that.message == null;
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (from != null ? from.hashCode() : 0);
        result = 31 * result + (to != null ? to.hashCode() : 0);
        result = 31 * result + (imported ? 1 : 0);
        result = 31 * result + (message != null ? message.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("type", type)
                .add("from", from)
                .add("to", to)
                .add("imported", imported)
                .add("message", message)
                .toString();
    }
}
