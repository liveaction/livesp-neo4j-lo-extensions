package com.livingobjects.neo4j.model.header;

import com.livingobjects.neo4j.model.PropertyType;

public class SimpleElementHeader extends HeaderElement {
    SimpleElementHeader(String elementName, String propertyName, PropertyType type, boolean isArray, int idx) {
        super(elementName, propertyName, type, isArray, idx);
    }

    @Override
    public boolean isSimple() {
        return true;
    }

    @Override
    public <R> R visit(Visitor<R> visitor) {
        return visitor.visitSimple(this);
    }
}
