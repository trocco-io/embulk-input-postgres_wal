package org.embulk.input.postgresql_wal;

import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.SchemaConfig;
import org.embulk.spi.unit.ToStringMap;

public interface PluginTask
        extends Task {
    @Config("options")
    @ConfigDefault("{}")
    public ToStringMap getOptions();

    @Config("host")
    public String getHost();

    @Config("port")
    public Integer getPort();

    @Config("database")
    public String getDatabase();

    @Config("schema")
    public String getSchema();

    @Config("user")
    public String getUser();

    @Config("password")
    public String getPassword();

    // if you get schema from config
    @Config("columns")
    public SchemaConfig getColumns();

    @Config("enable_metadata_deleted")
    @ConfigDefault("true")
    boolean getEnableMetadataDeleted();

    @Config("enable_metadata_fetched_at")
    @ConfigDefault("true")
    boolean getEnableMetadataFetchedAt();

    @Config("enable_metadata_seq")
    @ConfigDefault("true")
    boolean getEnableMetadataSeq();

    @Config("metadata_prefix")
    @ConfigDefault("\"_\"")
    String getMetadataPrefix();

    @Config("default_timezone")
    @ConfigDefault("\"UTC\"")
    String getDefaultTimezone();

    @Config("slot")
    String getSlot();

    @Config("to_lsn")
    String getToLsn();

}
