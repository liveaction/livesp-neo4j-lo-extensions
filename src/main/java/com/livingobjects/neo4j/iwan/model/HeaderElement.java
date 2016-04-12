package com.livingobjects.neo4j.iwan.model;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class HeaderElement {
    public static final Pattern COLUMN_PATTERN = Pattern.compile("([^:]*:[^:]*)\\.([^:]*):?(\\w+)?");

    public enum Type {STRING, NUMBER, BOOLEAN, DATE}

    public final String elementName;
    public final String propertyName;
    public final Type type;
    public final int index;

    public HeaderElement(String elementName, String propertyName, Type type, int idx) {
        this.elementName = elementName;
        this.propertyName = propertyName;
        this.type = type;
        this.index = idx;
    }

    public static HeaderElement of(String columnName, int idx) {
        Matcher m = COLUMN_PATTERN.matcher(columnName);
        if (!m.matches()) {
            throw new IllegalArgumentException("The header '" + columnName + "' don't match pattern !");
        }
        return new HeaderElement(m.group(1), m.group(2),
                Optional.ofNullable(m.group(3)).map(String::toUpperCase).map(Type::valueOf).orElse(Type.STRING), idx);
    }
}
