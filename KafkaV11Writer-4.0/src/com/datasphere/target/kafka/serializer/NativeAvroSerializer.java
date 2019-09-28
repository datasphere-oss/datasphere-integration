package com.datasphere.target.kafka.serializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import com.datasphere.intf.Formatter;
import com.datasphere.kafka.schemaregistry.SchemaRegistry;
import com.datasphere.proc.BaseFormatter;
import com.datasphere.proc.events.HDEvent;
import com.datasphere.source.lib.constant.Constant;
import com.datasphere.source.lib.intf.*;
import java.nio.*;

public class NativeAvroSerializer implements KafkaMessageSerilaizer
{
    protected Formatter formatter;
    private SchemaRegistry schemaRegistryClient;
    private Map<String, Integer> tableToSchemaRegistryId;
    private Constant.FormatOptions formatOption;
    
    public NativeAvroSerializer(final Formatter formatter, final SchemaRegistry schemaRegistryClient) {
        this.tableToSchemaRegistryId = new HashMap<String, Integer>();
        this.formatOption = Constant.FormatOptions.Native;
        this.schemaRegistryClient = schemaRegistryClient;
        this.formatter = formatter;
        if (((BaseFormatter)formatter).getFormatterProperties().containsKey("formatAs")) {
            this.formatOption = Constant.FormatOptions.valueOf(((BaseFormatter)formatter).getFormatterProperties().get("formatAs").toString());
        }
    }
    
    @Override
    public byte[] convertToBytes(final Object data) throws Exception {
        final HDEvent cdcEvent = (HDEvent)data;
        final Integer schemaId = this.getSchemaId(cdcEvent);
        if (schemaId == null) {
            return null;
        }
        return this.formatData(data, schemaId);
    }
    
    protected byte[] formatData(final Object data, final int schemaId) throws Exception {
        final byte[] hdEventBytes = this.formatter.format(data);
        final ByteBuffer idLengthAndDataBuffer = ByteBuffer.allocate(8 + hdEventBytes.length);
        idLengthAndDataBuffer.putInt(hdEventBytes.length + 4);
        idLengthAndDataBuffer.putInt(schemaId);
        idLengthAndDataBuffer.put(hdEventBytes);
        return idLengthAndDataBuffer.array();
    }
    
    public Integer getSchemaId(final HDEvent cdcEvent) throws Exception {
        String tableName = (String)cdcEvent.metadata.get("TableName");
        final Object operationType = cdcEvent.metadata.get(com.datasphere.common.constants.Constant.OPERATION_TYPE);
        final String operationName = (String)cdcEvent.metadata.get(com.datasphere.common.constants.Constant.OPERATION);
        final String operationSubName = (String)cdcEvent.metadata.get(com.datasphere.common.constants.Constant.OPERRATION_SUB_NAME);
        int schemaId;
        if (operationType != null && !((String)operationType).isEmpty() && ((String)operationType).equalsIgnoreCase(com.datasphere.common.constants.Constant.DDL_OPERATION)) {
            if ((cdcEvent.metadata.get(com.datasphere.common.constants.Constant.CATALOG_OBJECT_TYPE).toString().equalsIgnoreCase(com.datasphere.common.constants.Constant.TABLE_OBJ_TYPE) && (operationName.equalsIgnoreCase(com.datasphere.common.constants.Constant.ALTER_OPERATION) || operationName.equalsIgnoreCase(com.datasphere.common.constants.Constant.CREATE_OPERATION) || operationName.equalsIgnoreCase(com.datasphere.common.constants.Constant.RENAME_OPERATION))) || operationSubName.equalsIgnoreCase(com.datasphere.common.constants.Constant.CREATE_TABLE_AS)) {
                final String catalogName = (cdcEvent.metadata.get("CatalogName") != null) ? (cdcEvent.metadata.get("CatalogName") + ".") : "";
                final String schemaName = (cdcEvent.metadata.get("SchemaName") != null) ? (cdcEvent.metadata.get("SchemaName") + ".") : "";
                tableName = catalogName + schemaName + cdcEvent.metadata.get("ObjectName");
                ((SchemaCreater)this.formatter).createSchemaForTable((Object)cdcEvent);
                this.tableToSchemaRegistryId.put(tableName, ((SchemaCreater)this.formatter).getSchemaRegistryId(tableName, this.schemaRegistryClient));
            }
            if (this.formatOption.equals((Object)Constant.FormatOptions.Table)) {
                return null;
            }
            if (!this.tableToSchemaRegistryId.containsKey("DDLRecord")) {
                ((SchemaCreater)this.formatter).createSchemaForControlRecord("DDLRecord");
                this.tableToSchemaRegistryId.put("DDLRecord", ((SchemaCreater)this.formatter).getSchemaRegistryId("DDLRecord", this.schemaRegistryClient));
            }
            schemaId = this.tableToSchemaRegistryId.get("DDLRecord");
        }
        else if (operationName.equalsIgnoreCase(com.datasphere.common.constants.Constant.BEGIN_OPERATION) || operationName.equalsIgnoreCase(com.datasphere.common.constants.Constant.COMMIT_OPERATION)) {
            if (this.formatOption.equals((Object)Constant.FormatOptions.Table)) {
                return null;
            }
            if (!this.tableToSchemaRegistryId.containsKey("ControlRecord")) {
                ((SchemaCreater)this.formatter).createSchemaForControlRecord("ControlRecord");
                this.tableToSchemaRegistryId.put("ControlRecord", ((SchemaCreater)this.formatter).getSchemaRegistryId("ControlRecord", this.schemaRegistryClient));
            }
            schemaId = this.tableToSchemaRegistryId.get("ControlRecord");
        }
        else {
            if (!this.tableToSchemaRegistryId.containsKey(tableName)) {
                ((SchemaCreater)this.formatter).createSchemaForTable((Object)cdcEvent);
                this.tableToSchemaRegistryId.put(tableName, ((SchemaCreater)this.formatter).getSchemaRegistryId(tableName, this.schemaRegistryClient));
            }
            schemaId = this.tableToSchemaRegistryId.get(tableName);
        }
        return schemaId;
    }
}
