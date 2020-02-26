package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableList;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.model.export.query.ColumnOrder;
import com.livingobjects.neo4j.model.export.query.Pair;
import org.neo4j.graphdb.Node;

import java.util.Comparator;
import java.util.List;

public final class LineageListSortComparator implements Comparator<List<Lineage>> {
    private final ImmutableList<Pair<Integer, ColumnOrder>> sort;
    private final Comparator<List<Lineage>> comparator;

    public LineageListSortComparator(ImmutableList<Pair<Integer, ColumnOrder>> sort, Comparator<List<Lineage>> comparator) {
        this.sort = sort;
        this.comparator = comparator;
    }

    @Override
    public int compare(List<Lineage> l1, List<Lineage> l2) {
        int compare = 0;
        for (Pair<Integer, ColumnOrder> order : sort) {
            Node node1 = l1.get(order.first).nodesByType.get(order.second.column.keyAttribute);
            Node node2 = l2.get(order.first).nodesByType.get(order.second.column.keyAttribute);
            if (node1 != null) {
                if (node2 != null) {
                    String value1 = PropertyConverter.asNonNullString(l1.get(order.first).getProperty(order.second.column.keyAttribute, order.second.column.property));
                    String value2 = PropertyConverter.asNonNullString(l2.get(order.first).getProperty(order.second.column.keyAttribute, order.second.column.property));
                    if (order.second.direction == ColumnOrder.Direction.ASC) {
                        compare = value1.toLowerCase().compareTo(value2.toLowerCase());
                    } else {
                        compare = value2.toLowerCase().compareTo(value1.toLowerCase());
                    }
                    if (compare != 0) {
                        break;
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
        if (compare == 0) {
            compare = comparator.compare(l1, l2);
        }
        return compare;
    }

}
