package org.embulk.input.postgresql_wal;

import com.google.gson.JsonElement;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnConfig;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.json.JsonParser;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

public class PostgresqlWalColumnVisitor implements ColumnVisitor {
    private static final String DEFAULT_TIMESTAMP_PATTERN = "%Y-%m-%d %H:%M:%S";
    private final Logger logger = LoggerFactory.getLogger(PostgresqlWalColumnVisitor.class);

    private final PageBuilder pageBuilder;
    private final PluginTask pluginTask;
    private final PostgresqlWalAccessor accessor;

    public PostgresqlWalColumnVisitor(final PostgresqlWalAccessor accessor, final PageBuilder pageBuilder, final PluginTask pluginTask) {
        this.accessor = accessor;
        this.pageBuilder = pageBuilder;
        this.pluginTask = pluginTask;
    }

    @Override
    public void stringColumn(Column column) {
        try {
            org.embulk.input.postgresql_wal.model.Column col = accessor.get(column.getName());
            String data = col.getValue();
            // assume input-postgresql plugin use json type for hstore
            // convert hstore to json to support same function
            if (col.getType().equals("hstore")){
                data = PostgresqlWalUtil.hstoreToJson(col.getValue());
            }
            if (Objects.isNull(data)) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setString(column, data);
            }
        } catch (Exception e) {
            pageBuilder.setNull(column);
        }
    }

    @Override
    public void booleanColumn(Column column) {
        try {
            org.embulk.input.postgresql_wal.model.Column col = accessor.get(column.getName());
            String data = col.getValue();
            if (Objects.isNull(data)) {
                pageBuilder.setNull(column);
            }else{
                pageBuilder.setBoolean(column, Boolean.parseBoolean(data));
            }
        } catch (Exception e) {
            pageBuilder.setNull(column);
        }
    }

    @Override
    public void longColumn(Column column) {
        try {
            org.embulk.input.postgresql_wal.model.Column col = accessor.get(column.getName());
            String data = col.getValue();
            if (col.getType().equals("money")){
                data = PostgresqlWalUtil.extractNumeric(data);
            }
            if (Objects.isNull(data)) {
                pageBuilder.setNull(column);
            }else{
                pageBuilder.setLong(column, Long.parseLong(data));
            }
        } catch (Exception e) {
            pageBuilder.setNull(column);
        }
    }

    @Override
    public void doubleColumn(Column column) {
        try {
            org.embulk.input.postgresql_wal.model.Column col = accessor.get(column.getName());
            String data = col.getValue();
            if (col.getType().equals("money")){
                data = PostgresqlWalUtil.extractNumeric(data);
            }
            if (Objects.isNull(data)) {
                pageBuilder.setNull(column);
            }else{
                pageBuilder.setDouble(column, Double.parseDouble(data));
            }
        } catch (Exception e) {
            pageBuilder.setNull(column);
        }
    }

    @Override
    public void timestampColumn(Column column) {
        try {
            Timestamp result;
            // meta_fetched_at need microsecond
            if (column.getName().equals(PostgresqlWalUtil.getFetchedAtName(this.pluginTask))) {
                result = Timestamp.ofInstant(Instant.now());
            } else {
                List<ColumnConfig> columnConfigs = pluginTask.getColumns().getColumns();
                String pattern = DEFAULT_TIMESTAMP_PATTERN;
                for (ColumnConfig config : columnConfigs) {
                    if (config.getName().equals(column.getName())
                            && config.getConfigSource() != null
                            && config.getConfigSource().getObjectNode() != null
                            && config.getConfigSource().getObjectNode().get("format") != null
                            && config.getConfigSource().getObjectNode().get("format").isTextual()) {
                        pattern = config.getConfigSource().getObjectNode().get("format").asText();
                        break;
                    }
                }
                TimestampParser parser = TimestampParser.of(pattern, pluginTask.getDefaultTimezone());
                org.embulk.input.postgresql_wal.model.Column col = accessor.get(column.getName());
                result = parser.parse(col.getValue());
            }
            pageBuilder.setTimestamp(column, result);
        } catch (Exception e) {
            pageBuilder.setNull(column);
        }
    }

    @Override
    public void jsonColumn(Column column) {
        try {
            org.embulk.input.postgresql_wal.model.Column col = accessor.get(column.getName());
            JsonElement data;
            if (col.getType().equals("hstore")){
                String jsonStr = PostgresqlWalUtil.hstoreToJson(col.getValue());
                data =  new com.google.gson.JsonParser().parse(jsonStr);
            }else{
                data = new com.google.gson.JsonParser().parse(col.getValue());
            }
            if (data.isJsonNull() || data.isJsonPrimitive()) {
                pageBuilder.setNull(column);
            } else {
                pageBuilder.setJson(column, new JsonParser().parse(data.toString()));
            }
        } catch (Exception e) {
            pageBuilder.setNull(column);
        }
    }
}
