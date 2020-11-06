package org.embulk.input.postgresql_wal.model;

import org.postgresql.replication.LogSequenceNumber;

public class AbstractWalEvent {
    private LogSequenceNumber logSequenceNumber;
    private LogSequenceNumber nextLogSequenceNumber;

    public LogSequenceNumber getNextLogSequenceNumber() {
        return nextLogSequenceNumber;
    }

    public void setNextLogSequenceNumber(LogSequenceNumber nextLogSequenceNumber) {
        this.nextLogSequenceNumber = nextLogSequenceNumber;
    }

    public LogSequenceNumber getLogSequenceNumber() {
        return logSequenceNumber;
    }

    public void setLogSequenceNumber(LogSequenceNumber logSequenceNumber) {
        this.logSequenceNumber = logSequenceNumber;
    }
}