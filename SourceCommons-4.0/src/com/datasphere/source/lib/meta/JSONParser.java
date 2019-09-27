package com.datasphere.source.lib.meta;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import com.datasphere.common.errors.Error;
import com.datasphere.common.exc.AdapterException;
import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.source.lib.constant.Constant;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONParser
{
    String metaInput;
    JsonNode node;
    JsonNode fields;
    Column column;
    Map<String, Column[]> colMap;
    
    public JSONParser(final String metaDatafilename) throws AdapterException {
        this.metaInput = null;
        this.node = null;
        this.fields = null;
        this.column = null;
        this.colMap = new HashMap<String, Column[]>();
        try {
            final FileInputStream jsonFile = new FileInputStream(metaDatafilename);
            final ObjectMapper mapper = ObjectMapperFactory.newInstance();
            this.loadColMap(null, this.node = mapper.readTree((InputStream)jsonFile));
            this.setJSONMetaFields();
            jsonFile.close();
        }
        catch (IOException ioExp) {
            throw new AdapterException(Error.GENERIC_EXCEPTION, (Throwable)ioExp);
        }
    }
    
    public Column[] getColumns(final String path) {
        return this.colMap.get(path);
    }
    
    private Column createColumnObject(final JsonNode node) throws AdapterException {
        if (node.isContainerNode() && !node.isArray()) {
            final String name = node.get("name").textValue();
            final String dataType = node.get("type").textValue();
            final int position = node.get("position").intValue();
            int size = -1;
            if (node.get("size") != null) {
                size = node.get("size").intValue();
            }
            Column column;
            if (dataType.equals("INTEGER")) {
                column = new IntegerColumn();
            }
            else if (dataType.equals("DOUBLE")) {
                column = new DoubleColumn();
            }
            else if (dataType.equals("FLOAT")) {
                column = new FloatColumn();
            }
            else if (dataType.equals("LONG")) {
                column = new LongColumn();
            }
            else if (dataType.equals("SHORT")) {
                column = new ShortColumn();
            }
            else if (dataType.equals("STRING")) {
                column = new StringColumn();
            }
            else {
                if (!dataType.equals("BYTE")) {
                    final String errmsg = "Column type [" + dataType + "] is not supported";
                    throw new AdapterException(Error.UNSUPORTED_DATATYPE, errmsg);
                }
                column = new ByteColumn();
            }
            if (size != -1) {
                column.setSize(size);
            }
            column.setName(name);
            column.setIndex(position);
            return column;
        }
        throw new AdapterException(Error.INVALID_JSON_NODE);
    }
    
    private void loadColMap(String path, final JsonNode node) throws AdapterException {
        final Iterator<String> fieldNames = (Iterator<String>)node.fieldNames();
        if (fieldNames.hasNext()) {
            while (fieldNames.hasNext()) {
                final String field = fieldNames.next();
                if (field == "Ver") {
                    final String ver = new Integer(node.get(field).intValue()).toString();
                    if (path != null) {
                        path = path + "." + ver;
                    }
                    else {
                        path = ver;
                    }
                }
                else if (path != null) {
                    path = path + "." + field;
                }
                else {
                    path = field;
                }
                if (field != "fields") {
                    final JsonNode childNode = node.get(field);
                    this.loadColMap(path, childNode);
                }
                else {
                    if (!node.isContainerNode() || node.isArray()) {
                        continue;
                    }
                    final JsonNode childNode = node.get(field);
                    final int size = childNode.size();
                    if (size != 0) {
                        final Column[] fields = new Column[childNode.size()];
                        for (int idx = 0; idx < childNode.size(); ++idx) {
                            fields[idx] = this.createColumnObject(childNode.get(idx));
                        }
                        this.colMap.put(path, fields);
                    }
                    else {
                        System.out.println("No field definition found for [" + path + "]");
                    }
                }
            }
        }
        else if (node.isArray()) {
            for (int idx2 = 0; idx2 < node.size(); ++idx2) {
                final JsonNode childNode = node.get(idx2);
                this.loadColMap(path, childNode);
            }
        }
    }
    
    public void setJSONMetaFields() throws JsonProcessingException, IOException {
        this.fields = this.getJSONMetaFields();
    }
    
    public JsonNode getJSONMetaFields() throws JsonProcessingException, IOException {
        return this.fields = this.node.get("fields");
    }
    
    public int getFieldCount() {
        return this.fields.size();
    }
    
    public Column fillColumnMetaData(final Hashtable<Integer, Column> columnMetaData, final int endian, final boolean nullTerminatedString, final int colIndex) {
        String dataType = null;
        String name = null;
        int index = 0;
        int size = 0;
        if (this.fields != null) {
            this.node = this.fields.get(colIndex);
            if (this.node.has("name")) {
                name = this.node.get("name").textValue();
            }
            if (this.node.has("type")) {
                dataType = this.node.get("type").textValue();
            }
            if (this.node.has("position")) {
                index = this.node.get("position").intValue();
            }
            if (this.node.has("size")) {
                size = this.node.get("size").intValue();
            }
            if (endian == 1) {
                if (dataType.equals("INTEGER")) {
                    (this.column = new IntegerColumn()).setType(Constant.fieldType.INTEGER);
                    this.column.setSize(Constant.INTEGER_SIZE);
                }
                else if (dataType.equals("DOUBLE")) {
                    (this.column = new DoubleColumn()).setType(Constant.fieldType.DOUBLE);
                    this.column.setSize(Constant.DOUBLE_SIZE);
                }
                else if (dataType.equals("FLOAT")) {
                    (this.column = new FloatColumn()).setType(Constant.fieldType.FLOAT);
                    this.column.setSize(Constant.INTEGER_SIZE);
                }
                else if (dataType.equals("LONG")) {
                    (this.column = new LongColumn()).setType(Constant.fieldType.LONG);
                    this.column.setSize(Constant.DOUBLE_SIZE);
                }
                else if (dataType.equals("SHORT")) {
                    (this.column = new ShortColumn()).setType(Constant.fieldType.SHORT);
                    this.column.setSize(Constant.SHORT_SIZE);
                }
                else if (!nullTerminatedString && dataType.equals("STRING")) {
                    (this.column = new StringColumn()).setType(Constant.fieldType.STRING);
                }
            }
            else if (dataType.equals("INTEGER")) {
                (this.column = new LEIntegerColumn()).setType(Constant.fieldType.INTEGER);
                this.column.setSize(Constant.INTEGER_SIZE);
            }
            else if (dataType.equals("DOUBLE")) {
                (this.column = new LEDoubleColumn()).setType(Constant.fieldType.DOUBLE);
                this.column.setSize(Constant.DOUBLE_SIZE);
            }
            else if (dataType.equals("FLOAT")) {
                (this.column = new LEFloatColumn()).setType(Constant.fieldType.FLOAT);
                this.column.setSize(Constant.INTEGER_SIZE);
            }
            else if (dataType.equals("LONG")) {
                (this.column = new LELongColumn()).setType(Constant.fieldType.LONG);
                this.column.setSize(Constant.DOUBLE_SIZE);
            }
            else if (dataType.equals("SHORT")) {
                (this.column = new ShortColumn()).setType(Constant.fieldType.SHORT);
                this.column.setSize(Constant.SHORT_SIZE);
            }
            else if (!nullTerminatedString && dataType.equals("STRING")) {
                (this.column = new LEStringColumn()).setType(Constant.fieldType.STRING);
                this.column.setSize(size);
            }
            if (dataType.equals("BYTE")) {
                (this.column = new ByteColumn()).setType(Constant.fieldType.BYTE);
                this.column.setSize(Constant.BYTE_SIZE);
            }
            if (nullTerminatedString && dataType.equals("STRING")) {
                (this.column = new StringColumnNullTerminated()).setType(Constant.fieldType.STRING);
            }
            this.column.setIndex(index);
            this.column.setName(name);
            columnMetaData.put(colIndex, this.column);
        }
        return this.column;
    }
}
