package com.livingobjects.neo4j.model.export;

import com.google.common.collect.ImmutableList;
import com.livingobjects.neo4j.helper.PropertyConverter;
import com.livingobjects.neo4j.model.export.query.ColumnOrder;
import org.neo4j.graphdb.Node;

import java.util.Comparator;

public final class LineageSortComparator implements Comparator<Lineage> {
    private final ImmutableList<ColumnOrder> sort;
    private final Comparator<Lineage> comparator;

    public LineageSortComparator(ImmutableList<ColumnOrder> sort, Comparator<Lineage> comparator) {
        this.sort = sort;
        this.comparator = comparator;
    }

    @Override
    public int compare(Lineage l1, Lineage l2) {
        int compare = 0;
        for (ColumnOrder columnOrder : sort) {
            Node node1 = l1.nodesByType.get(columnOrder.column.keyAttribute);
            Node node2 = l2.nodesByType.get(columnOrder.column.keyAttribute);
            if (node1 != null) {
                if (node2 != null) {
                    String value1 = PropertyConverter.asNonNullString(l1.getProperty(columnOrder.column.keyAttribute, columnOrder.column.property));
                    String value2 = PropertyConverter.asNonNullString(l2.getProperty(columnOrder.column.keyAttribute, columnOrder.column.property));
                    if (columnOrder.direction == ColumnOrder.Direction.ASC) {
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
