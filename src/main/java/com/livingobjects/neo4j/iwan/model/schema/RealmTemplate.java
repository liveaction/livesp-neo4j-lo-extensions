package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.base.MoreObjects;

import java.util.Objects;
import java.util.Set;

public final class RealmTemplate {

    public String name;

    public Set<RealmPathElement> pathElements;

    public Set<MemdexPathRelation> memdexPaths;

    public RealmTemplate() {
    }

    public RealmTemplate(
            String name,
            Set<RealmPathElement> pathElements,
            Set<MemdexPathRelation> memdexPaths) {
        this.name = name;
        this.pathElements = pathElements;
        this.memdexPaths = memdexPaths;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<RealmPathElement> getPathElements() {
        return pathElements;
    }

    public void setPathElements(Set<RealmPathElement> pathElements) {
        this.pathElements = pathElements;
    }

    public Set<MemdexPathRelation> getMemdexPaths() {
        return memdexPaths;
    }

    public void setMemdexPaths(Set<MemdexPathRelation> memdexPaths) {
        this.memdexPaths = memdexPaths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RealmTemplate that = (RealmTemplate) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(pathElements, that.pathElements) &&
                Objects.equals(memdexPaths, that.memdexPaths);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, pathElements, memdexPaths);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("name", name)
                .add("pathElements", pathElements)
                .add("memdexPaths", memdexPaths)
                .toString();
    }
}
