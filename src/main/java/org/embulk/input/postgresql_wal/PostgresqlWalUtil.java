package org.embulk.input.postgresql_wal;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.postgresql.util.HStoreConverter;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PostgresqlWalUtil {
    private static Pattern numericRegex = Pattern.compile("[.0-9]+");
    private static final ObjectMapper mapper = new ObjectMapper();

    public static String getDeleteFlagName(PluginTask task) {
        return task.getMetadataPrefix() + "deleted";
    }

    public static String getFetchedAtName(PluginTask task) {
        return task.getMetadataPrefix() + "fetched_at";
    }

    public static String getSeqName(PluginTask task) {
        return task.getMetadataPrefix() + "seq";
    }

    public static AtomicLong getSeqCounter() {
        return SeqCounterHolder.INSTANCE;
    }

    public static String hstoreToJson(String value){
        try {
            Map map = HStoreConverter.fromString(value);
            return mapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    public static class SeqCounterHolder {
        private static final AtomicLong INSTANCE = new AtomicLong(0);
    }

    public static String extractNumeric(String str){
        Matcher m = numericRegex.matcher(str);
        if (m.find()){
            return m.group(0);
        }else{
            return null;
        }
    }
}
