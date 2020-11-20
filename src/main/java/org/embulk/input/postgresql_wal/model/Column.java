package org.embulk.input.postgresql_wal.model;

import org.embulk.input.postgresql_wal.PostgresqlWalUtil;

public class Column {
    private String name;
    private String type;
    private String value;

    public Column(String name, String type, String value) {
        this.name = name;
        this.type = type;
        this.value = value;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        if (this.type.equals("money")){
            return PostgresqlWalUtil.extractNumeric(this.value);
        }else{
            return value;
        }
    }

    public void setValue(String value) {
        this.value = value;

    }
}
