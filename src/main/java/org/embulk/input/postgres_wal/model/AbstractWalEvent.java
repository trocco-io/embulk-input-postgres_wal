package org.embulk.input.postgres_wal.model;

import org.postgresql.replication.LogSequenceNumber;

public class AbstractWalEvent {
    private LogSequenceNumber logSequenceNumber;

    public LogSequenceNumber getLogSequenceNumber() {
        return logSequenceNumber;
    }

    public void setLogSequenceNumber(LogSequenceNumber logSequenceNumber) {
        this.logSequenceNumber = logSequenceNumber;
    }
}