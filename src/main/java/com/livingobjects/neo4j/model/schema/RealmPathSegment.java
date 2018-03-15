package com.livingobjects.neo4j.model.schema;

import com.google.common.base.MoreObjects;

public final class RealmPathSegment {

    public final String path;

    public final String keyAttribute;

    public RealmPathSegment(String path, String keyAttribute) {
        this.path = path;
        this.keyAttribute = keyAttribute;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RealmPathSegment that = (RealmPathSegment) o;

        if (path != null ? !path.equals(that.path) : that.path != null) return false;
        return keyAttribute != null ? keyAttribute.equals(that.keyAttribute) : that.keyAttribute == null;
    }

    @Override
    public int hashCode() {
        int result = path != null ? path.hashCode() : 0;
        result = 31 * result + (keyAttribute != null ? keyAttribute.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("path", path)
                .add("keyAttribute", keyAttribute)
                .toString();
    }
}
