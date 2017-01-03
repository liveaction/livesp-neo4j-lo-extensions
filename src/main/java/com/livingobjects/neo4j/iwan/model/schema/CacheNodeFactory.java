package com.livingobjects.neo4j.iwan.model.schema;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.UnmodifiableIterator;
import com.livingobjects.neo4j.iwan.model.UniqueEntity;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.time.Instant;
import java.util.Iterator;
import java.util.Map;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.CREATED_AT;

public final class CacheNodeFactory {

    public final Map<ImmutableSet<String>, Node> nodesByKeys = Maps.newHashMap();

    public final GraphDatabaseService graphDB;

    public final Label label;

    public final ImmutableList<Label> extraLabels;

    public final ImmutableSet<String> keys;

    public static CacheNodeFactory of(GraphDatabaseService graphDB, ImmutableList<String> labels, ImmutableSet<String> keys) {
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("At least one label is required to create nodes");
        }
        Label label = DynamicLabel.label(labels.get(0));
        ImmutableList.Builder<Label> extraLabelsBuilder = ImmutableList.builder();
        for (int index = 1; index < labels.size(); index++) {
            String extraLabel = labels.get(index);
            extraLabelsBuilder.add(DynamicLabel.label(extraLabel));
        }
        ImmutableList<Label> extraLabels = extraLabelsBuilder.build();
        return new CacheNodeFactory(label, graphDB, extraLabels, keys);
    }

    public CacheNodeFactory(Label label, GraphDatabaseService graphDB, ImmutableList<Label> extraLabels, ImmutableSet<String> keys) {
        this.label = label;
        this.graphDB = graphDB;
        this.extraLabels = extraLabels;
        this.keys = keys;
        initialLoad();
    }

    private ImmutableSet<String> readNode(Node node) {
        ImmutableSet.Builder<String> values = ImmutableSet.builder();
        for (String key : keys) {
            String value = getNullableStringProperty(node, key);
            if (value != null) {
                values.add(value);
            }
        }
        return values.build();
    }

    protected Node createNode(ImmutableSet<String> keyValues) {
        if (!keyValues.isEmpty()) {
            Node node = graphDB.createNode();
            node.addLabel(label);

            for (Label extraLabel : extraLabels) {
                node.addLabel(extraLabel);
            }

            UnmodifiableIterator<String> keysIterator = keys.iterator();
            UnmodifiableIterator<String> keyValuesIterator = keyValues.iterator();
            while (keysIterator.hasNext() && keyValuesIterator.hasNext()) {
                String name = keysIterator.next();
                String value = keyValuesIterator.next();
                node.setProperty(name, value);
            }

            node.setProperty(CREATED_AT, Instant.now().toEpochMilli());
            return node;
        } else {
            throw new IllegalArgumentException("Empty keyValues : cannot create node");
        }
    }

    private void initialLoad() {
        Iterator<Node> nodes = graphDB.findNodes(label);
        while (nodes.hasNext()) {
            Node node = nodes.next();
            ImmutableSet<String> keys = readNode(node);
            if (keys != null) {
                nodesByKeys.put(keys, node);
            }
        }
    }

    public UniqueEntity<Node> getOrCreate(ImmutableSet<String> keys) {
        Node node = nodesByKeys.get(keys);
        if (node == null) {
            node = createNode(keys);
            nodesByKeys.put(keys, node);
            return UniqueEntity.created(node);
        } else {
            return UniqueEntity.existing(node);
        }
    }

    private String getNullableStringProperty(Node node, String propertyName) {
        Object prop = node.getProperty(propertyName, null);
        return (prop == null) ? null : prop.toString();
    }
}
