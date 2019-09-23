package com.datasphere.hdstore.elasticsearch;

import com.datasphere.hdstore.base.*;
import org.apache.log4j.*;
import com.datasphere.persistence.*;
import org.jctools.queues.*;
import com.datasphere.hdstore.constants.*;
import com.datasphere.runtime.components.*;
import com.datasphere.common.exc.*;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.action.bulk.*;
import java.util.concurrent.*;
import org.elasticsearch.common.unit.*;
import com.datasphere.hdstore.*;
import com.fasterxml.jackson.core.*;
import org.elasticsearch.client.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.*;
import org.elasticsearch.action.index.*;

public class DataType extends DataTypeBase<HDStore>
{
    private static final Logger logger;
    private static final int QUEUED_HDS_MIN = 100;
    private static final int QUEUED_HDS_MAX = 1024;
    private static final long QUEUED_HDS_TIMEOUT_MS = 1000L;
    private static final long BACKGROUND_THREAD_TIMEOUT = 3000L;
    private final BlockingQueue<HD> queuedHDs;
    private final BackgroundTask backgroundTask;
    private Map<String, Object> idPropertiesMap;
    
    DataType(final HDStore hdStore, final String name, final JsonNode schema, final HStore ws, final Map<String, Object> idPropertiesMap) {
        super(hdStore, name, schema);
        this.queuedHDs = new MpscCompoundQueue<HD>(1024);
        this.idPropertiesMap = null;
        this.idPropertiesMap = idPropertiesMap;
        this.backgroundTask = new BackgroundTask(hdStore.getName(), this.getName(), 3000L, ws) {
            @Override
            public void run() {
                final List<HD> hds = new ArrayList<HD>(1024);
                while (!this.isTerminationRequested()) {
                    this.waitForSignal();
                    boolean first = true;
                    while (true) {
                        DataType.this.queuedHDs.drainTo(hds);
                        if (hds.size() <= (first ? 0 : 100)) {
                            break;
                        }
                        DataType.this.insert(hds, this.getHDStoreObject());
                        hds.clear();
                        first = false;
                    }
                }
                DataType.this.queuedHDs.drainTo(hds);
                if (!hds.isEmpty()) {
                    DataType.this.insert(hds, this.getHDStoreObject());
                }
            }
        };
    }
    
    @Override
    public synchronized boolean insert(final List<HD> hds, final HStore ws) {
        if (Constants.IsEsRollingIndexEnabled.equalsIgnoreCase("true")) {
            final HDStore hdStore = this.getHDStore();
            final HDStoreManager manager = (HDStoreManager)hdStore.getManager();
            final IndexOperationsManager indexManager = manager.getIndexManager();
            indexManager.checkForRollingIndex(hdStore);
        }
        boolean result = false;
        final String errorMessage = null;
        Exception processingException = null;
        int hdCount = 0;
        long elapsedTime = 0L;
        try {
            final BulkRequestBuilder bulkRequest = this.createBulkInsertRequest(hds);
            hdCount = bulkRequest.numberOfActions();
            if (hdCount > 0) {
                final BulkResponse bulkResponse = (BulkResponse)bulkRequest.execute().actionGet();
                elapsedTime = bulkResponse.getTookInMillis();
                if (bulkResponse.hasFailures()) {
                    if (ws != null) {
                        String failureReason = bulkResponse.buildFailureMessage();
                        if (bulkResponse.buildFailureMessage().contains("UnavailableShardsException")) {
                            failureReason = failureReason + " because elasticsearch index " + this.getHDStore().getIndexName() + " is throwing UnavailableShardsException. Please check the index status";
                        }
                        DataType.logger.warn((Object)("Failed to insert hds into " + ws.getMetaFullName() + failureReason));
                        ws.notifyAppMgr(EntityType.HDSTORE, ws.getMetaName(), ws.getMetaID(), (Exception)new SystemException("Failed to insert hds into " + ws.getMetaFullName() + "..." + failureReason), "Insert into hd store failure", new Object[0]);
                    }
                    return false;
                }
                result = true;
            }
        }
        catch (MapperParsingException | JsonProcessingException ex2) {
        }
        this.reportInsertResults(result, hdCount, errorMessage, processingException, elapsedTime);
        return result;
    }
    
    private BulkRequestBuilder createBulkInsertRequest(final Iterable<HD> hds) throws JsonProcessingException {
        final HDStore hdStore = this.getHDStore();
        final HDStoreManager manager = (HDStoreManager)hdStore.getManager();
        final String hdStoreName = this.getName();
        final Client client = manager.getClient();
        final BulkRequestBuilder bulkRequest = client.prepareBulk();
        bulkRequest.setTimeout(new TimeValue(Constants.ELASTICSEARCH_INTIAL_TIME_TO_WAIT_UNTIL_INDEX_IS_READY, TimeUnit.MINUTES));
        final ObjectMapper mapper = Utility.objectMapper;
        Map<String, Object> hdMap = new HashMap<String, Object>(1);
        for (final HD hd : hds) {
            if (this.isValid(hd)) {
                final String indexName = hdStore.getIndexName();
                hdMap = (Map<String, Object>)mapper.treeToValue((TreeNode)hd, (Class)hdMap.getClass());
                String id = null;
                if (this.idPropertiesMap != null) {
                    final String mappedField = this.idPropertiesMap.get("_id").get("path");
                    if (mappedField == null) {
                        throw new IllegalStateException("No path mapping found for id");
                    }
                    id = (String)hdMap.get(mappedField);
                    if (id == null) {
                        throw new IllegalStateException("No field with name " + mappedField + " found to map the id");
                    }
                }
                IndexRequestBuilder index = null;
                if (id == null) {
                    index = client.prepareIndex(indexName, hdStoreName);
                }
                else {
                    index = client.prepareIndex(indexName, hdStoreName, id);
                }
                bulkRequest.add(index.setSource((Map)hdMap));
                if (!DataType.logger.isDebugEnabled()) {
                    continue;
                }
                DataType.logger.debug((Object)String.format("Inserting HD into HDStore '%s' - '%s'", hdStoreName, hd));
            }
        }
        return bulkRequest;
    }
    
    private void reportInsertResults(final boolean result, final int hdCount, final String errorMessage, final Exception processingException, final long elapsedTime) {
        if (processingException != null) {
            DataType.logger.warn((Object)String.format("Caught mapping exception - %s", processingException.getMessage()));
        }
        if (!result) {
            DataType.logger.error((Object)String.format("Failed to insert %d HDs - %s", hdCount, errorMessage));
        }
        if (elapsedTime > 750L && DataType.logger.isInfoEnabled()) {
            final String hdStoreName = this.getHDStore().getName();
            DataType.logger.info((Object)String.format("Inserted %d HDs successfully into HDStore '%s' in %d ms", hdCount, hdStoreName, elapsedTime));
        }
        if (result && DataType.logger.isDebugEnabled()) {
            final String hdStoreName = this.getHDStore().getName();
            DataType.logger.debug((Object)String.format("Inserted %d HDs successfully into HDStore '%s' in %d ms", hdCount, hdStoreName, elapsedTime));
        }
    }
    
    @Override
    public void queue(final HD hd) {
        if (!this.backgroundTask.isAlive()) {
            this.backgroundTask.start();
        }
        if (this.queuedHDs.remainingCapacity() <= 1) {
            this.backgroundTask.signalBackgroundThread();
        }
        while (true) {
            try {
                this.queuedHDs.put(hd);
            }
            catch (InterruptedException ex) {
                continue;
            }
            break;
        }
    }
    
    @Override
    public void queue(final Iterable<HD> hds) {
        for (final HD hd : hds) {
            this.queue(hd);
        }
    }
    
    @Override
    public void flush() {
        this.flushQueuedHDs();
        final HDStore hdStore = this.getHDStore();
        hdStore.flush();
    }
    
    @Override
    public void fsync() {
        this.flushQueuedHDs();
        final HDStore hdStore = this.getHDStore();
        hdStore.fsync();
    }
    
    private void flushQueuedHDs() {
        if (this.backgroundTask.isAlive()) {
            this.backgroundTask.terminate();
        }
    }
    
    static {
        logger = Logger.getLogger((Class)DataType.class);
    }
}
