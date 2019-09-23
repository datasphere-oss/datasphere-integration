package com.datasphere.hdstore;

import java.util.*;
import com.datasphere.persistence.*;
import com.datasphere.hdstore.exceptions.*;
import com.fasterxml.jackson.databind.*;

public enum InternalType
{
    CHECKPOINT("$Internal.CHECKPOINT", "{  \"context\": [     { \"name\": \"$id\", \"type\": \"string\", \"nullable\": false },    { \"name\": \"$timestamp\", \"type\": \"datetime\", \"nullable\": false },    { \"name\": \"NodeName\", \"type\": \"string\", \"nullable\": false },    { \"name\": \"HDStoreName\", \"type\": \"string\", \"nullable\": false },    { \"name\": \"HDCount\", \"type\": \"long\", \"nullable\": false },    { \"name\": \"PathCount\", \"type\": \"long\", \"nullable\": false },    { \"name\": \"Paths\", \"type\": \"binary64\", \"nullable\": false }  ],  \"metadata\": [ \"id\" ]}"), 
    MONITORING("$Internal.MONITORING", "{  \"context\": [     { \"name\": \"serverID\", \"type\": \"string\", \"nullable\": false , \"index\": \"true\" },    { \"name\": \"entityID\", \"type\": \"string\", \"nullable\": false , \"index\": \"true\" },    { \"name\": \"valueLong\", \"type\": \"long\", \"nullable\": true , \"index\": \"false\" },    { \"name\": \"valueString\", \"type\": \"string\", \"nullable\": true , \"index\": \"false\" },    { \"name\": \"timeStamp\", \"type\": \"long\", \"nullable\": false , \"index\": \"true\" },    { \"name\": \"type\", \"type\": \"string\", \"nullable\": false , \"index\": \"false\" }  ]}"), 
    HEALTH("$Internal.HEALTH", "{  \"context\": [     { \"name\": \"id\", \"type\": \"string\", \"nullable\": false , \"index\": \"true\" }  ]}");
    
    public final String hdStoreName;
    public final String schema;
    
    private InternalType(final String hdStoreName, final String schema) {
        this.hdStoreName = hdStoreName;
        this.schema = schema;
    }
    
    public DataType getDataType(final HDStoreManager manager, final Map<String, Object> properties) {
        final HDStore hdStore = manager.getOrCreate(this.hdStoreName, properties);
        final String dataTypeName = this.name();
        final JsonNode dataTypeSchema = Utility.readTree(this.schema);
        final DataType result = hdStore.setDataType(dataTypeName, dataTypeSchema, null);
        if (result == null) {
            throw new HDStoreException(String.format("Unable to create data type '%s' in HDStore '%s'", dataTypeName, this.hdStoreName));
        }
        return result;
    }
    
    public String getHDStoreName() {
        return this.hdStoreName;
    }
}
