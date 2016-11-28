package com.livingobjects.neo4j.iwan.model.export;

import com.google.common.collect.ImmutableList;
import org.neo4j.graphdb.Node;

import java.util.Comparator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.TAG;

public final class LineageComparator implements Comparator<Lineage> {
    private final ImmutableList<String> attributesOrdering;

    public LineageComparator(ImmutableList<String> attributesOrdering) {
        this.attributesOrdering = attributesOrdering;
    }

    @Override
    public int compare(Lineage l1, Lineage l2) {
        int compare = -1;
        for (String attribute : attributesOrdering) {
            Node node1 = l1.nodesByType.get(attribute);
            Node node2 = l2.nodesByType.get(attribute);
            if (node1 != null) {
                if (node2 != null) {
                    String tag1 = node1.getProperty(TAG).toString();
                    String tag2 = node2.getProperty(TAG).toString();
                    compare = tag1.compareTo(tag2);
                } else {
                    compare = -1;
                }
            } else {
                if (node2 != null) {
                    compare = 1;
                } else {
                    compare = 0;
                }
            }
            if (compare != 0) {
                return compare;
            }
        }
        return compare;
    }
}
