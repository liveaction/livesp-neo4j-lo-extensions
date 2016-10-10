package com.livingobjects.neo4j.iwan.model.export;

import java.util.Comparator;

import static com.livingobjects.neo4j.iwan.model.IwanModelConstants.*;

public final class PropertyNameComparator implements Comparator<String> {

    public static final PropertyNameComparator PROPERTY_NAME_COMPARATOR = new PropertyNameComparator();

    private PropertyNameComparator() {
    }

    @Override
    public int compare(String o1, String o2) {
        int result = propertyPrecedence(o1) - propertyPrecedence(o2);
        if (result == 0) {
            return o1.compareTo(o2);
        } else {
            return result;
        }
    }

    public int propertyPrecedence(String prop) {
        switch (prop) {
            case TAG:
                return 0;
            case ID:
                return 1;
            case NAME:
                return 2;
            default:
                return 3;
        }
    }

}
