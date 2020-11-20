package org.embulk.input.postgresql_wal.model;

import java.util.List;

public class AbstractRowEvent extends AbstractWalEvent {
    private String schemaName;
    private String tableName;
    private List<Column> primaryKeyColumns;
    private List<Column> columns;

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public EventType getEventType() {
        return null;
    }

    public void setPrimaryKeyColumns(List<Column> primaryKeyColumns) {
        this.primaryKeyColumns = primaryKeyColumns;
    }

    public List<Column> getPrimaryKeyColumns(){
        return this.primaryKeyColumns;
    }

    public List<Column> getColumns() {
        return columns;
    }

    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }


    public Column getColumn(String name){
        return this.columns.stream().filter(column -> column.getName().equals(name)).findFirst().orElseGet(null);
    }
}