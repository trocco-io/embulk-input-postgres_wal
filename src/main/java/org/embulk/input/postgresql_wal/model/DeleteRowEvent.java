package org.embulk.input.postgresql_wal.model;

import java.util.Map;

public class DeleteRowEvent extends AbstractRowEvent {
    private Map<String, String> primaryKeys;

    @Override
    public EventType getEventType() {
        return EventType.DELETE;
    }

    public Map<String, String> getPrimaryKeys() {
        return primaryKeys;
    }

    public void setPrimaryKeys(Map<String, String> primaryKeys) {
        this.primaryKeys = primaryKeys;
    }
}
