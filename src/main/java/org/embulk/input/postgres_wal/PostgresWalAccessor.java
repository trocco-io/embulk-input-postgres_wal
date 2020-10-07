package org.embulk.input.postgres_wal;

import java.util.Map;

public class PostgresWalAccessor {
    private final Map<String, String> row;

    public PostgresWalAccessor(final Map<String, String> row){
        this.row = row;
    }

    public String get(String name){
        return row.getOrDefault(name, null);
    }
}
