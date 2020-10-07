package org.embulk.input.postgres_wal.model;

import java.util.Map;

public class UpdateRowEvent extends AbstractRowEvent {
    private Map<String, String> fields;
    private Map<String, String> primaryKeys;

    @Override
    public EventType getEventType() {
        return EventType.UPDATE;
    }

    public Map<String, String> getFields() {
        return fields;
    }

    public void setFields(Map<String, String> fields) {
        this.fields = fields;
    }

    public Map<String, String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(Map<String, String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}
