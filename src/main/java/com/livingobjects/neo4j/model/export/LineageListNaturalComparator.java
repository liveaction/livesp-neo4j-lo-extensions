package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableSet;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.model.iwan.GraphModelConstants;
import org.neo4j.graphdb.Node;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public final class LineageListNaturalComparator implements Comparator<List<Lineage>> {
    private final ImmutableSet<String> attributesOrdering;

    public LineageListNaturalComparator(ImmutableSet<String> attributesOrdering) {
        this.attributesOrdering = attributesOrdering;
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Override
    public int compare(List<Lineage> l1, List<Lineage> l2) {
        int compare = -1;
        int size = Math.min(l1.size(), l2.size()); // sizes should be the same
        for (int i = 0; i < size; i++) {
            for (String attribute : attributesOrdering) {
                Node node1 = l1.get(i).nodesByType.get(attribute);
                Node node2 = l2.get(i).nodesByType.get(attribute);
                if (node1 != null) {
                    if (node2 != null) {
                        String tag1 = PropertyConverter.asNonNullString(l1.get(i).getProperty(attribute, GraphModelConstants.TAG));
                        String tag2 = PropertyConverter.asNonNullString(l2.get(i).getProperty(attribute, GraphModelConstants.TAG));
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
        }
        return compare;
    }
}
