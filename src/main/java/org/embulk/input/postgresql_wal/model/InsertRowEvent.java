package org.embulk.input.postgresql_wal.model;

import java.util.Map;

public class InsertRowEvent extends AbstractRowEvent {
    private Map<String, String> fields;

    @Override
    public EventType getEventType() {
        return EventType.INSERT;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }
}
