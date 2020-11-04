package org.embulk.input.postgresql_wal.decoders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.embulk.input.postgresql_wal.model.*;
import org.postgresql.replication.LogSequenceNumber;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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
    public AbstractRowEvent decode(final ByteBuffer data, final LogSequenceNumber logSequenceNumber) {
        // TODO: should return array of row event?
        AbstractRowEvent result = new AbstractRowEvent();
        String jsonStr = StandardCharsets.UTF_8.decode(data).toString();
        try {
            JsonNode node = mapper.readTree(jsonStr);
            JsonNode changeNode = node.get("change");
            System.out.println(changeNode.isArray());
            System.out.println(node);
            System.out.println(changeNode);
            System.out.println(changeNode.isArray());
            if (changeNode.isArray() && changeNode.size() >= 1) {
                System.out.println("before loop ================");
                System.out.println(changeNode.size());
                for (JsonNode dataNode : changeNode) {
                    System.out.println("loop ================");
                    System.out.println(dataNode);
                    System.out.println(dataNode.has("kind"));
                    System.out.println("chekc ================");
                    if (!dataNode.has("kind")) {
                        continue;
                    }
                    if (dataNode.get("kind").asText().equals(EventType.INSERT.getString())) {
                        return decodeInsertEvent(dataNode);
                    } else if (dataNode.get("kind").asText().equals(EventType.UPDATE.getString())) {
                        return decodeUpdateEvent(dataNode);
                    } else if (dataNode.get("kind").asText().equals(EventType.DELETE.getString())) {
                        return decodeDeleteEvent(dataNode);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        result.setLogSequenceNumber(logSequenceNumber);
        return result;
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
    public InsertRowEvent decodeInsertEvent(JsonNode node) {
        InsertRowEvent rowEvent = new InsertRowEvent();
        setMeta(rowEvent, node);
        rowEvent.setFields(makePair(node, "columnnames", "columnvalues"));
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
    public UpdateRowEvent decodeUpdateEvent(JsonNode node) {
        UpdateRowEvent rowEvent = new UpdateRowEvent();
        setMeta(rowEvent, node);
        rowEvent.setFields(makePair(node, "columnnames", "columnvalues"));
        rowEvent.setPrimaryKeys(makePair(node.get("oldkeys"), "columnnames", "columnvalues"));
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
    public DeleteRowEvent decodeDeleteEvent(JsonNode node) {
        DeleteRowEvent rowEvent = new DeleteRowEvent();
        setMeta(rowEvent, node);
        rowEvent.setPrimaryKeys(makePair(node.get("oldkeys"), "columnnames", "columnvalues"));
        return rowEvent;
    }

    private void setMeta(AbstractRowEvent rowEvent, JsonNode node) {
        rowEvent.setSchemaName(node.get("schema").asText());
        rowEvent.setTableName(node.get("table").asText());
    }

    private ArrayList<String> covertArrayString(JsonNode node, String name) {
        ArrayList<String> values = new ArrayList<>();
        for (JsonNode n : node.get(name)) {
            values.add(n.asText());
        }
        return values;
    }

    private Map<String, String> makePair(JsonNode node, String keyName, String valueName) {
        ArrayList<String> columnNames = covertArrayString(node, keyName);
        ArrayList<String> columnValues = covertArrayString(node, valueName);
        Map<String, String> pair = new HashMap<>();
        for (int i = 0; i < Math.max(columnNames.size(), columnValues.size()); i++) {
            pair.put(columnNames.get(i), columnValues.get(i));
        }

        return pair;
    }

    class ArrayStringWrapper {
        private List<String> values;
    }
}

