package org.embulk.input.postgresql_wal.decoders;

import org.embulk.input.postgresql_wal.model.AbstractRowEvent;
import org.postgresql.replication.LogSequenceNumber;

import java.nio.ByteBuffer;
import java.util.List;

public interface DecodingPlugin {
    List<AbstractRowEvent> decode(ByteBuffer data, LogSequenceNumber logSequenceNumber);
}
