package com.livingobjects.neo4j;

import com.livingobjects.neo4j.loader.MetaSchema;
import org.neo4j.graphdb.Transaction;

public final class InnerTransaction {
    public final Transaction tx;
    public final MetaSchema metaSchema;

    public InnerTransaction(Transaction tx, MetaSchema metaSchema) {
        this.tx = tx;
        this.metaSchema = metaSchema;
    }

    public InnerTransaction(Transaction tx) {
        this(tx, new MetaSchema(tx));
    }
}
