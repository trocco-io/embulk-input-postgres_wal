package org.embulk.input.postgresql_wal;

import org.embulk.input.postgresql_wal.model.Column;

import java.util.List;
import java.util.Map;

public class PostgresqlWalAccessor {
    private final List<Column> columns;

    public PostgresqlWalAccessor(final List<Column> columns) {
        this.columns = columns;
    }

    public Column get(String name) {
        Column col = columns.stream().filter(column -> column.getName().equals(name)).findFirst().orElseGet(null);
        if (col == null){
            return null;
        }
        return col;
    }
}
