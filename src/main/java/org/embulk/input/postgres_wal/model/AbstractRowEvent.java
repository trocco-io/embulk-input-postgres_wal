package org.embulk.input.postgres_wal.model;

public class AbstractRowEvent  extends AbstractWalEvent {

    private String schemaName;

    private String tableName;

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

    public EventType getEventType(){
        return null;
    }

}