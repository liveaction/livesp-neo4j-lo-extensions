package com.livingobjects.neo4j.iwan.model.schema;

public final class SchemaResult {

    public int updated;

    public SchemaResult() {
    }

    public SchemaResult(int updated) {
        this.updated = updated;
    }

    public int getUpdated() {
        return updated;
    }

    public void setUpdated(int updated) {
        this.updated = updated;
    }
}
