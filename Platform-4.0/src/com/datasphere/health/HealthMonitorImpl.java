package com.datasphere.health;

import org.apache.log4j.*;
import com.datasphere.runtime.monitor.*;
import com.fasterxml.jackson.databind.*;
import com.datasphere.hdstore.*;
import java.util.*;
import com.datasphere.uuid.*;
import com.datasphere.uuid.UUID;

public class HealthMonitorImpl implements HealthMonitor
{
    private static Logger logger;
    private DataType monitorDataType;
    private HDStoreManager monitorDataStore;
    
    public HealthMonitorImpl() {
        this.monitorDataType = MonitorModel.getHealthRecordDataType();
        this.monitorDataStore = MonitorModel.getHealthRecordDataStore();
    }
    
    @Override
    public HealthRecordCollection getHealthRecordsByCount(final int size, final long from) throws Exception {
        if (this.monitorDataStore == null || this.monitorDataType == null) {
            HealthMonitorImpl.logger.warn((Object)"Monitoring data store is not available for querying");
        }
        final String baseQuery = "{ \"select\":[ \"*\" ],\"from\":[ \"%s\" ],\"orderby\":[{\"attr\" : \"%s\", \"ascending\" : %b}]  }";
        final String query = String.format(baseQuery, this.monitorDataType.getHDStore().getName(), "startTime", Boolean.FALSE);
        HealthRecordCollection hrc = null;
        final JsonNode jsonNode = Utility.objectMapper.readTree(query);
        final HDQuery q = this.monitorDataStore.prepareQuery(jsonNode);
        final Iterable<HD> results = q.execute();
        hrc = new HealthRecordCollection();
        int currentSize = 0;
        for (final HD healthRecord : results) {
            if (currentSize++ == size) {
                break;
            }
            hrc.healthRecords.add(healthRecord);
        }
        return hrc;
    }
    
    @Override
    public HealthRecordCollection getHealthRecordsByTime(final long from, final long to) throws Exception {
        final String baseQuery = "{ \"select\": [ \"*\" ],\"from\":   [ \"%s\" ],\"where\":  { \"and\": [{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s },{ \"oper\": \"%s\", \"attr\": \"%s\", \"value\": %s }]}}";
        final String query = String.format(baseQuery, this.monitorDataType.getHDStore().getName(), "gt", "startTime", String.valueOf(from), "lt", "startTime", String.valueOf(to));
        HealthRecordCollection hrc = null;
        final JsonNode jsonNode = Utility.objectMapper.readTree(query);
        final HDQuery q = this.monitorDataStore.prepareQuery(jsonNode);
        final Iterable<HD> results = q.execute();
        hrc = new HealthRecordCollection();
        for (final HD healthRecord : results) {
            hrc.healthRecords.add(healthRecord);
        }
        return hrc;
    }
    
    @Override
    public HealthRecordCollection getHealtRecordById(final UUID uuid) throws Exception {
        final String baseQuery = "{ \"select\": [ \"*\" ],\"from\":   [ \"%s\" ],\"where\": { \"oper\": \"%s\", \"attr\": \"%s\", \"value\": \"%s\" }}";
        final String query = String.format(baseQuery, this.monitorDataType.getHDStore().getName(), "eq", "id", uuid.getUUIDString());
        HealthRecordCollection hrc = null;
        final JsonNode jsonNode = Utility.objectMapper.readTree(query);
        final HDQuery q = this.monitorDataStore.prepareQuery(jsonNode);
        final Iterable<HD> results = q.execute();
        hrc = new HealthRecordCollection();
        for (final HD healthRecord : results) {
            hrc.healthRecords.add(healthRecord);
        }
        return hrc;
    }
    
    @Override
    public HealthRecordCollection getHealthRecordsWithIssuesList(final long start, final long end) throws Exception {
        final String baseQuery = "{ \"select\": [ \"issuesList\",\"startTime\" ],\"from\":   [ \"%s\" ],\"orderby\":[{\"attr\" : \"startTime\", \"ascending\" : false}] , \"where\":  { \"and\": [{ \"oper\": \"gt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"lt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"neq\", \"attr\": \"issuesList\", \"value\": null }]}}";
        final String query = String.format(baseQuery, this.monitorDataType.getHDStore().getName(), String.valueOf(start), String.valueOf(end));
        final HealthRecordCollection hrc = new HealthRecordCollection();
        final Iterable<HD> results = this._executeQuery(query);
        for (final HD healthRecord : results) {
            hrc.healthRecords.add(healthRecord);
        }
        return hrc;
    }
    
    @Override
    public HealthRecordCollection getHealthRecordsWithStatusChangeList(final long start, final long end) throws Exception {
        final String baseQuery = "{ \"select\": [ \"stateChangeList\",\"startTime\" ],\"from\":   [ \"%s\" ],\"orderby\":[{\"attr\" : \"startTime\", \"ascending\" : false}] , \"where\":  { \"and\": [{ \"oper\": \"gt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"lt\", \"attr\": \"startTime\", \"value\": %s },{ \"oper\": \"neq\", \"attr\": \"stateChangeList\", \"value\": null }]}}";
        final String query = String.format(baseQuery, this.monitorDataType.getHDStore().getName(), String.valueOf(start), String.valueOf(end));
        final HealthRecordCollection hrc = new HealthRecordCollection();
        final Iterable<HD> results = this._executeQuery(query);
        for (final HD healthRecord : results) {
            hrc.healthRecords.add(healthRecord);
        }
        return hrc;
    }
    
    private Iterable<HD> _executeQuery(final String query) throws Exception {
        final HealthRecordCollection hrc = null;
        final JsonNode jsonNode = Utility.objectMapper.readTree(query);
        final HDQuery q = this.monitorDataStore.prepareQuery(jsonNode);
        final Iterable<HD> results = q.execute();
        return results;
    }
    
    static {
        HealthMonitorImpl.logger = Logger.getLogger((Class)HealthMonitorImpl.class);
    }
}
