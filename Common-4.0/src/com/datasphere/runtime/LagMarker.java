package com.datasphere.runtime;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.joda.time.DateTime;

import com.datasphere.event.ObjectMapperFactory;
import com.datasphere.utility.GenerateTableFormat;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class LagMarker implements KryoSerializable
{
    private UUID applicationUUID;
    public CopyOnWriteArrayList<LagInfo> lagLedger;
    
    public String key() {
        final StringBuilder sb = new StringBuilder();
        for (final LagInfo lagInfo : this.lagLedger) {
            sb.append(lagInfo.fromComponent + "#");
        }
        return sb.toString();
    }
    
    public LagMarker() {
        this.applicationUUID = UUID.nilUUID();
        this.lagLedger = new CopyOnWriteArrayList<LagInfo>();
        this.applicationUUID = UUID.nilUUID();
    }
    
    public LagMarker(final UUID applicationUUID) {
        this.applicationUUID = UUID.nilUUID();
        this.lagLedger = new CopyOnWriteArrayList<LagInfo>();
        this.applicationUUID = applicationUUID;
    }
    
    private LagMarker(final UUID applicationUUID, final CopyOnWriteArrayList<LagInfo> lagLedger) {
        this.applicationUUID = UUID.nilUUID();
        this.lagLedger = new CopyOnWriteArrayList<LagInfo>();
        this.lagLedger = (CopyOnWriteArrayList<LagInfo>)lagLedger.clone();
        this.applicationUUID = new UUID(applicationUUID.getUUIDString());
    }
    
    public void recordLag(final String nameOfComponent, final long timestamp) {
        final int size = this.lagLedger.size();
        if (size == 0) {
            final LagInfo lagInfo = new LagInfo(timestamp, nameOfComponent, null);
            this.lagLedger.add(lagInfo);
        }
        else {
            final LagInfo lagInfo = this.lagLedger.get(this.lagLedger.size() - 1);
            lagInfo.toComponent = nameOfComponent;
            final LagInfo lagInfo2 = new LagInfo(timestamp, nameOfComponent, null);
            this.lagLedger.add(lagInfo2);
        }
    }
    
    public Long calculateTotalLag() {
        if (this.lagLedger.isEmpty()) {
            return null;
        }
        if (this.lagLedger.size() > 2) {
            final LagInfo first = this.lagLedger.get(0);
            final LagInfo last = this.lagLedger.get(this.lagLedger.size() - 1);
            return last.timeStamp - first.timeStamp;
        }
        return null;
    }
    
    public void write(final Kryo kryo, final Output output) {
        output.writeLong(this.applicationUUID.time);
        output.writeLong(this.applicationUUID.clockSeqAndNode);
        output.writeLong((long)this.lagLedger.size());
        for (final LagInfo lagInfo : this.lagLedger) {
            output.writeLong((long)lagInfo.timeStamp);
            output.writeString(lagInfo.fromComponent);
            if (lagInfo.toComponent == null) {
                output.writeByte((byte)0);
            }
            else {
                output.writeByte((byte)1);
                output.writeString(lagInfo.toComponent);
            }
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        final long time = input.readLong();
        final long clockSeqAndNode = input.readLong();
        this.applicationUUID = new UUID(time, clockSeqAndNode);
        final long size = input.readLong();
        for (int i = 0; i < size; ++i) {
            final long timestamp = input.readLong();
            final String fromComponent = input.readString();
            String toComponent = null;
            final byte isNull = input.readByte();
            if (isNull == 1) {
                toComponent = input.readString();
            }
            this.lagLedger.add(new LagInfo(timestamp, fromComponent, toComponent));
        }
    }
    
    public boolean isEmpty() {
        return this.lagLedger.size() == 0;
    }
    
    public LagMarker copy() {
        return new LagMarker(this.applicationUUID, this.lagLedger);
    }
    
    public UUID getApplicationUUID() {
        return this.applicationUUID;
    }
    
    @Override
    public String toString() {
        final List<String> listOfColumnNames = new ArrayList<String>(Arrays.asList("Component Name", "Component Type", "Latency (ms)", "Server Name"));
        final List<List<String>> listOfRowData = new ArrayList<List<String>>();
        final StringBuilder stringBuffer = new StringBuilder();
        Long lastTimeStamp = null;
        for (final LagInfo entry : this.lagLedger) {
            final List<String> rowData = new ArrayList<String>();
            final Long val = entry.timeStamp;
            final DateTime dateTime = new DateTime((long)val);
            final String[] arr = entry.fromComponent.split("#");
            if (lastTimeStamp == null) {
                stringBuffer.append("Latency calculation started at component : " + arr[0] + " of type : " + arr[1] + " at time : " + dateTime.toString("yyyy-MM-dd hh:mm:ss"));
            }
            else {
                final String componentName = arr[0];
                final String componentType = arr[1];
                final String serverName = arr[2];
                final String latency = String.valueOf(entry.timeStamp - lastTimeStamp);
                rowData.add(componentName);
                rowData.add(componentType);
                rowData.add(latency);
                rowData.add(serverName);
            }
            lastTimeStamp = entry.timeStamp;
            listOfRowData.add(rowData);
        }
        stringBuffer.append(GenerateTableFormat.generateTable(listOfColumnNames, listOfRowData, 1));
        final Long lagCalculation = this.calculateTotalLag();
        if (lagCalculation != null) {
            stringBuffer.append("Total lag = ").append(lagCalculation).append(" milli seconds");
        }
        return stringBuffer.toString();
    }
    
    public String toJSON() {
        final ObjectMapper objectMapper = ObjectMapperFactory.newInstance();
        final Long lagCalculation = this.calculateTotalLag();
        final ObjectNode rootNode = objectMapper.createObjectNode();
        rootNode.put("rate", lagCalculation);
        rootNode.put("rate", lagCalculation);
        final ArrayNode fieldArray = objectMapper.createArrayNode();
        Long lastTimeStamp = null;
        for (final LagInfo entry : this.lagLedger) {
            final ObjectNode fieldNode = objectMapper.createObjectNode();
            final Long val = entry.timeStamp;
            final DateTime dateTime = new DateTime((long)val);
            final String[] arr = entry.fromComponent.split("#");
            final String componentName = arr[0];
            final String componentType = arr[1];
            final String serverName = arr[2];
            fieldNode.put("componentName", componentName);
            fieldNode.put("componentType", componentType);
            fieldNode.put("serverName", serverName);
            fieldNode.put("timestamp", entry.timeStamp);
            if (lastTimeStamp == null) {
                fieldNode.put("latency", 0);
            }
            else {
                final String latency = String.valueOf(entry.timeStamp - lastTimeStamp);
                fieldNode.put("latency", latency);
            }
            lastTimeStamp = entry.timeStamp;
            fieldArray.add((JsonNode)fieldNode);
        }
        rootNode.put("report", (JsonNode)fieldArray);
        return rootNode.toString();
    }
}
