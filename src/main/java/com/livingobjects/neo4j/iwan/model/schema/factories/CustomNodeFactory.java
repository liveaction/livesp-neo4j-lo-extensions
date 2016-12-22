package com.livingobjects.neo4j.iwan.model.schema.factories;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.UnmodifiableIterator;
import com.livingobjects.neo4j.iwan.model.CacheIndexedElementFactory;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

import java.time.Instant;
import java.util.Iterator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.CREATED_AT;

public class CustomNodeFactory extends CacheIndexedElementFactory<ImmutableSet<String>> {

    public final Label label;

    public final ImmutableList<Label> extraLabels;

    public final ImmutableSet<String> keys;

    public CustomNodeFactory(GraphDatabaseService graphDB, ImmutableList<String> labels, ImmutableSet<String> keys) {
        super(graphDB);
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("At least one label is required to create nodes");
        }
        this.label = DynamicLabel.label(labels.get(0));
        ImmutableList.Builder<Label> extraLabelsBuilder = ImmutableList.builder();
        for (int index = 1; index < labels.size(); index++) {
            String extraLabel = labels.get(index);
            extraLabelsBuilder.add(DynamicLabel.label(extraLabel));
        }
        this.extraLabels = extraLabelsBuilder.build();
        this.keys = keys;
    }

    @Override
    protected Iterator<Node> initialLoad() {
        return graphDB.findNodes(label);
    }

    @Override
    protected ImmutableSet<String> readNode(Node node) {
        ImmutableSet.Builder<String> values = ImmutableSet.builder();
        for (String key : keys) {
            String value = getNullableStringProperty(node, key);
            values.add(value);
        }
        return values.build();
    }

    @Override
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
}
