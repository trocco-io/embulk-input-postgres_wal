package org.embulk.input.postgres_wal.decoders;

import org.embulk.input.postgres_wal.model.AbstractWalEvent;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;

public interface DecodingPlugin {
    AbstractWalEvent decode(ByteBuffer data, LogSequenceNumber logSequenceNumber);
}
