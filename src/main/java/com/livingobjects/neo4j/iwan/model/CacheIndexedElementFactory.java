package com.livingobjects.neo4j.iwan.model;

import com.google.common.collect.Maps;
import org.neo4j.cypher.InvalidArgumentException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.CREATED_AT;

public abstract class CacheIndexedElementFactory<K> {

    public final Map<K, Node> attributes = Maps.newHashMap();

    public final GraphDatabaseService graphDB;

    public CacheIndexedElementFactory(GraphDatabaseService graphDB) {
        this.graphDB = graphDB;
        Iterator<Node> nodes = initialLoad();
        while (nodes.hasNext()) {
            Node node = nodes.next();
            K key = readNode(node);
            if (key != null) {
                attributes.put(key, node);
            }
        }
    }

    public UniqueEntity<Node> getOrCreate(K key) {
        Node node = attributes.get(key);
        if (node == null) {
            node = createNode(key);
            attributes.put(key, node);
            return UniqueEntity.created(node);
        } else {
            return UniqueEntity.existing(node);
        }
    }

    protected abstract Iterator<Node> initialLoad();

    protected abstract K readNode(Node node);

    protected abstract Node createNode(K attribute) throws InvalidArgumentException;

    protected Node createNodeFromStringKey(Label label, String keyName, String key) {
        Node node = graphDB.createNode();
        node.addLabel(label);
        node.setProperty(keyName, key);
        node.setProperty(CREATED_AT, Instant.now().toEpochMilli());
        return node;
    }

    protected String getNullableStringProperty(Node node, String propertyName) {
        Object prop = node.getProperty(propertyName, null);
        return (prop == null) ? null : prop.toString();
    }
}
