package com.livingobjects.neo4j;

public final class Neo4jLoadResult {

    public final long imported;

    public final long[] errorLines;

    public Neo4jLoadResult(long imported, long[] errorLines) {
        this.imported = imported;
        this.errorLines = errorLines;
    }
}
