package com.livingobjects.neo4j.model.exception;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class InsufficientContextException extends IllegalStateException {

    public final ImmutableSet<String> missingAttributesToChoose;

    public InsufficientContextException(String s, Set<String> missingAttributesToChoose) {
        super(s);
        this.missingAttributesToChoose = ImmutableSet.copyOf(missingAttributesToChoose);
    }

}
