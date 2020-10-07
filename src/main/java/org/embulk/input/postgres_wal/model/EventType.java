package org.embulk.input.postgres_wal.model;

public enum EventType {
    INSERT("insert"),
    UPDATE("update"),
    DELETE("delete");

    private final String text;

    private EventType(final String text) {
        this.text = text;
    }

    public String getString() {
        return this.text;
    }
}
