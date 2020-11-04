package org.embulk.input.postgresql_wal;

import java.util.Map;

public class PostgresqlWalAccessor {
    private final Map<String, String> row;

    public PostgresqlWalAccessor(final Map<String, String> row) {
        this.row = row;
    }

    public String get(String name) {
        return row.getOrDefault(name, null);
    }
}
