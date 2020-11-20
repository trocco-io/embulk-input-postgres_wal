package org.embulk.input.postgresql_wal.model;

import java.util.Map;

public class UpdateRowEvent extends AbstractRowEvent {
    @Override
    public EventType getEventType() {
        return EventType.UPDATE;
    }
}
