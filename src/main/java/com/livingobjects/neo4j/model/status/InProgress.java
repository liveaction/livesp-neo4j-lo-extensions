package com.livingobjects.neo4j.model.status;

public class InProgress extends Status {

    public InProgress() {
        super(Type.IN_PROGRESS);
    }

    @Override
    public <T> T visit(Status.Visitor<T> visitor) {
        return visitor.inProgress();
    }

}
