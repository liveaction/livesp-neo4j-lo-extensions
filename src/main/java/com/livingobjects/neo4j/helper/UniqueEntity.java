package com.livingobjects.neo4j.helper;

import java.util.function.Consumer;

public final class UniqueEntity<T> {
    public final boolean wasCreated;
    public final T entity;

    public static <T> UniqueEntity<T> created(T entity) {
        return new UniqueEntity<>(true, entity);
    }

    public static <T> UniqueEntity<T> existing(T entity) {
        return new UniqueEntity<>(false, entity);
    }

    private UniqueEntity(boolean wasCreated, T entity) {
        this.wasCreated = wasCreated;
        this.entity = entity;
    }

    public void wasCreated(Consumer<T> consumer) {
        if (this.wasCreated) {
            consumer.accept(entity);
        }
    }
}
