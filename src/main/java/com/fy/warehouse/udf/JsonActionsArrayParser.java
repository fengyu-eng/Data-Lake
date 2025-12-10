package com.fy.warehouse.udf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@PublicEvolving
public class JsonActionsArrayParser extends ScalarFunction {
    private static final ObjectMapper object_mapper = new ObjectMapper();
    private static final String DEFAULT_STRING = "";
    private static final long DEFAULT_LONG = 0L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonActionsArrayParser.class);
    @DataTypeHint("ROW<action_id STRING, item STRING, item_type STRING, ts BIGINT>")
    public Row eval(String jsonStr){
        Row resultRow = Row.of(DEFAULT_STRING, DEFAULT_STRING, DEFAULT_STRING, DEFAULT_LONG);
        if (jsonStr == null || jsonStr.trim().isEmpty()){
            LOG.warn("Input JSON string is null or empty");
            return resultRow;
        }
        try{
            JsonNode rootNode = object_mapper.readTree(jsonStr);
            if (!rootNode.isArray() || rootNode.size() == 0) {
                LOG.error("Invalid JSON format: expected non-empty array, but got {}", jsonStr);
                return resultRow;
            }
            JsonNode actionNode = rootNode.get(0);
            if (actionNode.has("action_id")) {
                resultRow.setField(0, actionNode.get("action_id").asText(DEFAULT_STRING));
            }
            if (actionNode.has("item")) {
                resultRow.setField(1, actionNode.get("item").asText(DEFAULT_STRING));
            }
            if (actionNode.has("item_type")) {
                resultRow.setField(2, actionNode.get("item_type").asText(DEFAULT_STRING));
            }
            if (actionNode.has("ts")) {
                resultRow.setField(3, actionNode.get("ts").asLong(DEFAULT_LONG));
            }
            return resultRow;
        } catch (Exception e) {
            LOG.error("Failed to parse JSON string: {}", jsonStr, e);
            return resultRow; // 返回默认值，避免下游null
        }
    }
}
