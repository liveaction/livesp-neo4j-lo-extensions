package com.livingobjects.neo4j.iwan.model;

public class SimpleElementHeader extends HeaderElement {
    SimpleElementHeader(String elementName, String propertyName, Type type, boolean isArray, int idx) {
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
