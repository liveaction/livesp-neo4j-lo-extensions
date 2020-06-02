package com.livingobjects.neo4j.loader;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

import java.util.List;
import java.util.function.BiConsumer;

public final class TransactionManager {
    private final GraphDatabaseService graphDb;

    public TransactionManager(GraphDatabaseService graphDb) {
        this.graphDb = graphDb;
    }

    public Transaction properlyRenewTransaction(Transaction tx, List<String[]> currentTransaction, BiConsumer<String[], Transaction> consumer) {
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
            tx.rollback();
        } else {
            tx.commit();
        }
        tx.close();
        return graphDb.beginTx();
    }

    public Transaction reloadValidTransactionLines(Transaction tx, List<String[]> lines, BiConsumer<String[], Transaction> consumer) {
        if (!lines.isEmpty()) {
            lines.forEach(l -> consumer.accept(l, tx));
            return renewTransaction(tx);
        }
        return tx;
    }
}
