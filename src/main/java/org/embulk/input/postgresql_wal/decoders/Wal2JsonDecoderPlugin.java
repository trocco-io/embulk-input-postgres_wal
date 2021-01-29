package org.embulk.input.postgresql_wal.decoders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.input.postgresql_wal.model.*;
import org.postgresql.replication.LogSequenceNumber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

// see wal2json format 1
// https://github.com/eulerto/wal2json


// {
//   "change":[
//      {
//         "kind":"update",
//         "schema":"public",
//         "table":"test",
//         "columnnames":[
//            "id",
//            "num",
//            "description",
//            "address"
//         ],
//         "columntypes":[
//            "integer",
//            "integer",
//            "text",
//            "text"
//         ],
//         "columnvalues":[
//            11,
//            133,
//            "dafs",
//            "hogehgoe"
//         ],
//         "oldkeys":{
//            "keynames":[
//               "id"
//            ],
//            "keytypes":[
//               "integer"
//            ],
//            "keyvalues":[
//               11
//            ]
//         }
//      }
//   ]
//}
public class Wal2JsonDecoderPlugin implements DecodingPlugin {
    private ObjectMapper mapper;

    public Wal2JsonDecoderPlugin() {
        mapper = new ObjectMapper();
    }

    @Override
    public List<AbstractRowEvent> decode(final ByteBuffer data, final LogSequenceNumber logSequenceNumber) {
        List<AbstractRowEvent> rows = new ArrayList<AbstractRowEvent>();
        String jsonStr = StandardCharsets.UTF_8.decode(data).toString();
        try {
            JsonNode node = mapper.readTree(jsonStr);
            LogSequenceNumber nextLsn = LogSequenceNumber.valueOf(node.get("nextlsn").asText());
            JsonNode changeNode = node.get("change");

            if (changeNode.isArray() && changeNode.size() >= 1) {
                for (JsonNode dataNode : changeNode) {
                    if (!dataNode.has("kind")) {
                        continue;
                    }
                    if (dataNode.get("kind").asText().equals(EventType.INSERT.getString())) {
                        rows.add(decodeInsertEvent(dataNode, nextLsn));
                    } else if (dataNode.get("kind").asText().equals(EventType.UPDATE.getString())) {
                        rows.add(decodeUpdateEvent(dataNode, nextLsn));
                    } else if (dataNode.get("kind").asText().equals(EventType.DELETE.getString())) {
                        rows.add(decodeDeleteEvent(dataNode, nextLsn));
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return rows;
    }

    // {
//   "change":[
//      {
//         "kind":"insert",
//         "schema":"public",
//         "table":"test",
//         "columnnames":[
//            "id",
//            "num",
//            "description",
//            "address"
//         ],
//         "columntypes":[
//            "integer",
//            "integer",
//            "text",
//            "text"
//         ],
//         "columnvalues":[
//            13,
//            3123,
//            "fdfa",
//            "fdwsf"
//         ]
//      }
//   ]
//}
    public InsertRowEvent decodeInsertEvent(JsonNode node, LogSequenceNumber lsn) {
        InsertRowEvent rowEvent = new InsertRowEvent();
        rowEvent.setNextLogSequenceNumber(lsn);
        setMeta(rowEvent, node);
        rowEvent.setColumns(makeColumns(node, "columnnames", "columntypes", "columnvalues"));
        return rowEvent;
    }

    // {
//   "change":[
//      {
//         "kind":"update",
//         "schema":"public",
//         "table":"test",
//         "columnnames":[
//            "id",
//            "num",
//            "description",
//            "address"
//         ],
//         "columntypes":[
//            "integer",
//            "integer",
//            "text",
//            "text"
//         ],
//         "columnvalues":[
//            1111,
//            1,
//            "dfas",
//            "fasdfa"
//         ],
//         "oldkeys":{
//            "keynames":[
//               "id"
//            ],
//            "keytypes":[
//               "integer"
//            ],
//            "keyvalues":[
//               1
//            ]
//         }
//      }
//   ]
//　}
    public UpdateRowEvent decodeUpdateEvent(JsonNode node, LogSequenceNumber lsn) {
        UpdateRowEvent rowEvent = new UpdateRowEvent();
        rowEvent.setNextLogSequenceNumber(lsn);
        setMeta(rowEvent, node);
        rowEvent.setColumns(makeColumns(node, "columnnames", "columntypes", "columnvalues"));
        rowEvent.setPrimaryKeyColumns(makeColumns(node.get("oldkeys"), "keynames", "keytypes", "keyvalues"));
        return rowEvent;
    }

    // {
//   "change":[
//      {
//         "kind":"delete",
//         "schema":"public",
//         "table":"test",
//         "oldkeys":{
//            "keynames":[
//               "id"
//            ],
//            "keytypes":[
//               "integer"
//            ],
//            "keyvalues":[
//               10
//            ]
//         }
//      }
//   ]
//}
    public DeleteRowEvent decodeDeleteEvent(JsonNode node, LogSequenceNumber lsn) {
        DeleteRowEvent rowEvent = new DeleteRowEvent();
        rowEvent.setNextLogSequenceNumber(lsn);
        setMeta(rowEvent, node);
        rowEvent.setPrimaryKeyColumns(makeColumns(node.get("oldkeys"), "keynames", "keytypes", "keyvalues"));
        return rowEvent;
    }

    private void setMeta(AbstractRowEvent rowEvent, JsonNode node) {
        rowEvent.setSchemaName(node.get("schema").asText());
        rowEvent.setTableName(node.get("table").asText());
    }

    private ArrayList<String> covertArrayString(JsonNode node, String name) {
        ArrayList<String> values = new ArrayList<>();
        for (JsonNode n : node.get(name)) {
            if (n.isNull()){
                values.add(null);
            }else{
                values.add(n.asText());
            }
        }
        return values;
    }

    private List<Column> makeColumns(JsonNode node, String keyName, String typeName, String valueName){
        ArrayList<String> columnNames = covertArrayString(node, keyName);
        ArrayList<String> columnTypes = covertArrayString(node, typeName);
        ArrayList<String> columnValues = covertArrayString(node, valueName);

        Integer[] sizeArray = {columnNames.size(),columnTypes.size(), columnValues.size()};
        Set<Integer> checkValue = new HashSet<Integer>(Arrays.asList(sizeArray));
        assert checkValue.size() == 1 : "key, value, type have to be same length";

        List<Column> columns = new ArrayList<>();
        for (int i = 0; i < columnNames.size(); i++){
            Column column = new Column(columnNames.get(i), columnTypes.get(i), columnValues.get(i));
            columns.add(column);
        }
        return columns;
    }
}

