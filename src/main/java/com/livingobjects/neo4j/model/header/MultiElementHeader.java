package com.livingobjects.neo4j.model.header;

public final class MultiElementHeader extends HeaderElement {
    public final String targetElementName;

    MultiElementHeader(String elementName, String targetElementName, String propertyName, Type type, boolean isArray, int idx) {
        super(elementName, propertyName, type, isArray, idx);
        this.targetElementName = targetElementName;
    }

    @Override
    public boolean isSimple() {
        return false;
    }

    @Override
    public <R> R visit(Visitor<R> visitor) {
        return visitor.visitMulti(this);
    }
}
