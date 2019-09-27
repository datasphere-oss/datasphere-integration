package com.datasphere.utils.writers.common;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.datasphere.uuid.UUID;
import com.datasphere.recovery.Position;

public class EventDataObject
{
    public static final String TYPE_INSERT = "INSERT";
    public static final String TYPE_UPDATE = "UPDATE";
    public static final String TYPE_DELETE = "DELETE";
    public static final String TYPE_SELECT = "SELECT";
    private final Map<String, Object> dataList;
    private final String type;
    private final Position pos;
    private final List<String> keys;
    private final Field[] schema;
    private UUID sourceUUID;
    private UUID typeUUID;
    private String sourceTableName;
    public Object[] data;
    public Map<String, Object> metadata;
    public Map<String, Object> userdata;
    public Object[] before;
    public byte[] beforePresenceBitMap;
    public byte[] dataPresenceBitMap;
    
    public EventDataObject(final Map<String, Object> dataList, final String type, final List<String> keys, final Position pos, final Field[] schema) {
        this.dataList = dataList;
        this.type = type;
        this.pos = pos;
        this.keys = keys;
        this.schema = schema;
    }
    
    public EventDataObject(final Map<String, Object> dataList, final String type, final List<String> keys, final Position pos, final Field[] schema, final UUID sourceUUID, final UUID typeUUID, final String tableName, final Object[] data, final Map<String, Object> metadata, final Map<String, Object> userdata, final Object[] before, final byte[] beforePresenceBitMap, final byte[] dataPresenceBitMap) {
        this.dataList = dataList;
        this.type = type;
        this.pos = pos;
        this.keys = keys;
        this.schema = schema;
        this.sourceUUID = sourceUUID;
        this.typeUUID = typeUUID;
        this.sourceTableName = tableName;
        this.data = data;
        this.metadata = metadata;
        this.userdata = userdata;
        this.before = before;
        this.beforePresenceBitMap = beforePresenceBitMap;
        this.dataPresenceBitMap = dataPresenceBitMap;
    }
    
    public Map<String, Object> getDataList() {
        return this.dataList;
    }
    
    public String getType() {
        return this.type;
    }
    
    public Position getPos() {
        return this.pos;
    }
    
    public List<String> getKeys() {
        return this.keys;
    }
    
    public Field[] getSchema() {
        return this.schema;
    }
    
    @Override
    public String toString() {
        return "EventDataObject [dataList=" + this.dataList + ", type=" + this.type + ", pos=" + this.pos + ", keys=" + this.keys + ", schema=" + Arrays.toString(this.schema) + "]";
    }
    
    public UUID getsourceUUID() {
        return this.sourceUUID;
    }
    
    public UUID gettargetUUID() {
        return this.typeUUID;
    }
    
    public String getSourceTable() {
        return this.sourceTableName;
    }
}
