package com.livingobjects.neo4j.model.export.query.filter;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = Filter.EmptyFilter.class, name = "empty"),
        @JsonSubTypes.Type(value = Filter.ColumnFilter.class, name = "property"),
        @JsonSubTypes.Type(value = Filter.AndFilter.class, name = "and"),
        @JsonSubTypes.Type(value = Filter.OrFilter.class, name = "or"),
        @JsonSubTypes.Type(value = Filter.NotFilter.class, name = "not"),
})
public abstract class Filter<T> {

    public abstract ImmutableList<T> columns();

    public abstract ImmutableList<ColumnFilter<T>> columnsFilters();

    public abstract boolean test(Function<T, Object> valueSupplier);

    Filter() {
    }

    static class EmptyFilter<T> extends Filter<T> {

        public EmptyFilter() {
        }

        @Override
        public ImmutableList<T> columns() {
            return ImmutableList.of();
        }

        @Override
        public ImmutableList<ColumnFilter<T>> columnsFilters() {
            return ImmutableList.of();
        }

        @Override
        public boolean test(Function<T, Object> valueSupplier) {
            return true;
        }

    }

    public static class ColumnFilter<T> extends Filter<T> {

        public final T column;
        public final ValueFilter valueFilter;

        public ColumnFilter(@JsonProperty("column") T column,
                            @JsonProperty("valueFilter") ValueFilter valueFilter) {
            this.column = column;
            this.valueFilter = valueFilter;
        }

        @Override
        public ImmutableList<T> columns() {
            return ImmutableList.of(column);
        }

        @Override
        public ImmutableList<ColumnFilter<T>> columnsFilters() {
            return ImmutableList.of(this);
        }

        @Override
        public boolean test(Function<T, Object> valueSupplier) {
            return valueFilter.test(valueSupplier.apply(column));
        }


    }

    public static class AndFilter<T> extends Filter<T> {

        private final ImmutableList<Filter<T>> filters;

        public AndFilter(@JsonProperty("filters") List<Filter<T>> filters) {
            this.filters = ImmutableList.copyOf(filters);
        }

        @Override
        public ImmutableList<T> columns() {
            return ImmutableList.copyOf(filters.stream()
                    .flatMap(f -> f.columns().stream())
                    .collect(Collectors.toList()));
        }

        @Override
        public ImmutableList<ColumnFilter<T>> columnsFilters() {
            return ImmutableList.copyOf(filters.stream()
                    .flatMap(f -> f.columnsFilters().stream())
                    .collect(Collectors.toList()));
        }

        @Override
        public boolean test(Function<T, Object> valueSupplier) {
            return filters.stream().allMatch(filter -> filter.test(valueSupplier));
        }

    }

    public static class OrFilter<T> extends Filter<T> {

        private final ImmutableList<Filter<T>> filters;

        public OrFilter(@JsonProperty("filters") List<Filter<T>> filters) {
            this.filters = ImmutableList.copyOf(filters);
        }

        @Override
        public ImmutableList<T> columns() {
            return ImmutableList.copyOf(filters.stream()
                    .flatMap(f -> f.columns().stream())
                    .collect(Collectors.toList()));
        }

        @Override
        public ImmutableList<ColumnFilter<T>> columnsFilters() {
            return ImmutableList.copyOf(filters.stream()
                    .flatMap(f -> f.columnsFilters().stream())
                    .collect(Collectors.toList()));
        }

        @Override
        public boolean test(Function<T, Object> valueSupplier) {
            return filters.stream().anyMatch(filter -> filter.test(valueSupplier));
        }

    }

    static class NotFilter<T> extends Filter<T> {

        private final Filter<T> filter;

        public NotFilter(@JsonProperty("filter") Filter<T> filter) {
            this.filter = filter;
        }

        @Override
        public ImmutableList<T> columns() {
            return filter.columns();
        }

        @Override
        public ImmutableList<ColumnFilter<T>> columnsFilters() {
            return null;
        }

        @Override
        public boolean test(Function<T, Object> valueSupplier) {
            return !filter.test(valueSupplier);
        }

    }

}
