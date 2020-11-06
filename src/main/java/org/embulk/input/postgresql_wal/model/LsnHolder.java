package org.embulk.input.postgresql_wal.model;

import org.postgresql.replication.LogSequenceNumber;

public class LsnHolder {
    private static LogSequenceNumber lsn;

    public static LogSequenceNumber getLsn() {
        return lsn;
    }

    public static void setLsn(LogSequenceNumber lsn) {
        LsnHolder.lsn = lsn;
    }
}
