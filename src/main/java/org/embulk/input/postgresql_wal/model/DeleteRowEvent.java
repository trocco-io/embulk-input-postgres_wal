package org.embulk.input.postgresql_wal.model;

import java.util.Map;

public class DeleteRowEvent extends AbstractRowEvent {
    @Override
    public EventType getEventType() {
        return EventType.DELETE;
    }
}
