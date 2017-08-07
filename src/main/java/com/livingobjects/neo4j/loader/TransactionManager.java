package com.livingobjects.neo4j.loader;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.function.Consumer;

public final class TransactionManager {
    private final GraphDatabaseService graphDb;

    public TransactionManager(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public Transaction properlyRenewTransaction(Transaction tx, List<String[]> currentTransaction, Consumer<String[]> consumer) {
        tx = renewTransaction(tx, true);
        tx = reloadValidTransactionLines(tx, currentTransaction, consumer);
        currentTransaction.clear();
        return tx;
    }

    public Transaction renewTransaction(Transaction tx) {
        return renewTransaction(tx, false);
    }

    public Transaction renewTransaction(Transaction tx, boolean asFailure) {
        if (asFailure) {
            tx.failure();
        } else {
            tx.success();
        }
        tx.close();
        return graphDb.beginTx();
    }

    public Transaction reloadValidTransactionLines(Transaction tx, List<String[]> lines, Consumer<String[]> consumer) {
        if (!lines.isEmpty()) {
            lines.forEach(consumer);
            return renewTransaction(tx);
        }
        return tx;
    }
}
