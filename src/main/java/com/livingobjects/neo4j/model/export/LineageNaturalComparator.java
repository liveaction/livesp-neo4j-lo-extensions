package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import org.neo4j.graphdb.Node;

import java.util.Comparator;

public final class LineageNaturalComparator implements Comparator<Lineage> {
    private final ImmutableSet<String> attributesOrdering;

    public LineageNaturalComparator(ImmutableSet<String> attributesOrdering) {
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
                    String tag1 = PropertyConverter.asNonNullString(l1.getProperty(attribute, GraphModelConstants.TAG));
                    String tag2 = PropertyConverter.asNonNullString(l2.getProperty(attribute, GraphModelConstants.TAG));
                    compare = tag1.compareTo(tag2);
                    if (compare == 0) {
                        compare = (int) (node1.getId() - node2.getId());
                    }
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
