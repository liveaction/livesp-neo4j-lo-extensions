package com.livingobjects.neo4j.iwan.model.schema.factories;

import com.livingobjects.neo4j.iwan.model.CacheIndexedElementFactory;
import org.neo4j.cypher.InvalidArgumentException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import java.time.Instant;
import java.util.Iterator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.CREATED_AT;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.LABEL_COUNTER;
import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.NAME;

public class CounterFactory extends CacheIndexedElementFactory<String> {

    public CounterFactory(GraphDatabaseService graphDB) {
        super(graphDB);
    }

    @Override
    protected Iterator<Node> initialLoad() {
        return graphDB.findNodes(LABEL_COUNTER);
    }

    @Override
    protected String readNode(Node node) {
        String context = getNullableStringProperty(node, "context");
        String name = getNullableStringProperty(node, NAME);
        if (context != null && name != null) {
            return context + '@' + name;
        } else {
            return null;
        }
    }

    @Override
    protected Node createNode(String counter) throws InvalidArgumentException {
        String[] split = counter.split("@");
        if (split.length == 2) {
            String context = split[0];
            String name = split[1];
            Node node = graphDB.createNode();
            node.addLabel(LABEL_COUNTER);
            node.setProperty("context", context);
            node.setProperty(NAME, name);
            node.setProperty(CREATED_AT, Instant.now().toEpochMilli());
            return node;
        } else {
            throw new IllegalArgumentException("Counter key must be of the form context@name : " + counter);
        }
    }
}
