package com.livingobjects.neo4j.model.export.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.escape.Escaper;
import com.google.common.escape.Escapers;

import javax.annotation.Nullable;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ValueFilter {

    private static final Pattern PATTERN = Pattern.compile("\\[(!?\\w+)](.*)");

    public static final Escaper ESCAPER = Escapers.builder()
            .addEscape('[', "\\[")
            .addEscape('\\', "\\\\")
            .addEscape('^', "\\^")
            .addEscape('$', "\\$")
            .addEscape('.', "\\.")
            .addEscape('|', "\\|")
            .addEscape('?', "\\?")
            .addEscape('*', "\\*")
            .addEscape('+', "\\+")
            .addEscape('(', "\\(")
            .addEscape(')', "\\)")
            .addEscape('{', "\\{")
            .addEscape('}', "\\}")
            .build();

    public enum Operator {
        lt, lte, gt, gte, eq, contains, regex, is_null
    }

    public final boolean not;
    public final Operator operator;
    public final Comparable value;

    private ValueFilter(boolean not,
                        Operator operator,
                        Object value) {
        this.not = not;
        this.operator = operator;
        if (value != null) {
            if (value instanceof Comparable) {
                this.value = (Comparable) value;
            } else {
                throw new IllegalArgumentException("Value must be a comparable : " + value);
            }
        } else {
            this.value = null;
        }
    }

    @JsonCreator
    public static ValueFilter fromString(String predicate) {
        if (predicate == null) {
            throw new IllegalArgumentException("Predicate must not be null");
        }
        Matcher matcher = PATTERN.matcher(predicate);
        if (matcher.matches()) {
            String arg = matcher.group(1);
            boolean not = false;
            if (arg.startsWith("!")) {
                not = true;
                arg = arg.substring(1);
            }
            Operator operator = Operator.valueOf(arg);
            String value = matcher.group(2);
            return new ValueFilter(not, operator, value);
        } else {
            return new ValueFilter(false, Operator.regex, "(?i).*" + ESCAPER.escape(predicate) + ".*");
        }
    }

    /**
     * Test value against this filter.
     *
     * @param value a nullable value
     * @return true if the value pass the filter test, false otherwise.
     */
    public boolean test(@Nullable Object value) {
        return FilterUtils.testFilter(value, this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ValueFilter that = (ValueFilter) o;
        return not == that.not &&
                operator == that.operator &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(not, operator, value);
    }

    @Override
    public String toString() {
        return String.format("[%s%s]%s", not ? "!" : "", operator, value);
    }

}
