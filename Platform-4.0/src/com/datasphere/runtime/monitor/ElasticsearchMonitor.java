package com.datasphere.runtime.monitor;

import org.apache.log4j.*;
import com.datasphere.hdstore.*;
import org.elasticsearch.action.admin.cluster.stats.*;
import com.datasphere.runtime.*;
import org.elasticsearch.action.admin.cluster.node.stats.*;
import org.elasticsearch.action.admin.cluster.node.hotthreads.*;
import com.datasphere.hdstore.elasticsearch.*;
import com.datasphere.hdstore.elasticsearch.HDStoreManager;

import org.elasticsearch.client.*;
import org.elasticsearch.monitor.fs.*;
import java.util.*;
import org.elasticsearch.transport.*;
import java.io.*;

public class ElasticsearchMonitor implements Runnable
{
    Long prevTimestamp;
    Long prevDocs;
    Long prevQueries;
    Long prevFetches;
    Long prevSize;
    
    public ElasticsearchMonitor() {
        this.prevTimestamp = null;
        this.prevDocs = null;
        this.prevQueries = null;
        this.prevFetches = null;
        this.prevSize = null;
        if (Logger.getLogger("Monitor").isDebugEnabled()) {
            Logger.getLogger("Monitor").debug((Object)"Initializing ElasticsearchMonitor");
        }
    }
    
    @Override
    public void run() {
        try {
            final List<MonitorEvent> monEvs = new ArrayList<MonitorEvent>();
            final long timestamp = System.currentTimeMillis();
            final HDStoreManager instance = HDStores.getAnyElasticsearchInstance();
            if (instance != null) {
                final Client client = instance.getClient();
                final ClusterStatsResponse clusterStats = (ClusterStatsResponse)client.admin().cluster().prepareClusterStats().get();
                final FsInfo.Path info = clusterStats.getNodesStats().getFs();
                monEvs.add(new MonitorEvent(BaseServer.baseServer.getServerID(), BaseServer.baseServer.getServerID(), MonitorEvent.Type.ELASTICSEARCH_FREE, info.getFree().getBytes() / 1000000000L + "GB", Long.valueOf(timestamp)));
                monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ELASTICSEARCH_FREE, info.getFree().getBytes() / 1000000000L + "GB", Long.valueOf(timestamp)));
                monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_FREE_BYTES, Long.valueOf(info.getFree().getBytes()), Long.valueOf(timestamp)));
                monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_TOTAL_BYTES, Long.valueOf(info.getTotal().getBytes()), Long.valueOf(timestamp)));
                final NodesStatsResponse nodestats = (NodesStatsResponse)client.admin().cluster().prepareNodesStats(new String[0]).get();
                long docs = 0L;
                long queries = 0L;
                long fetches = 0L;
                long size = 0L;
                for (final NodeStats n : nodestats.getNodes()) {
                    docs += n.getIndices().getDocs().getCount();
                    queries += n.getIndices().getSearch().getTotal().getQueryCount();
                    fetches += n.getIndices().getSearch().getTotal().getFetchCount();
                    size += n.getIndices().getStore().getSizeInBytes();
                }
                monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_DOCS, Long.valueOf(docs), Long.valueOf(timestamp)));
                monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_QUERIES, Long.valueOf(queries), Long.valueOf(timestamp)));
                monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_FETCHES, Long.valueOf(fetches), Long.valueOf(timestamp)));
                monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_SIZE_BYTES, Long.valueOf(size), Long.valueOf(timestamp)));
                if (this.prevDocs != null) {
                    final long delta = Math.abs(docs - this.prevDocs);
                    final Long rate = 1000L * delta / (timestamp - this.prevTimestamp);
                    monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_DOCS_RATE, rate, Long.valueOf(timestamp)));
                }
                this.prevDocs = docs;
                if (this.prevQueries != null) {
                    final long delta = Math.abs(queries - this.prevQueries);
                    final Long rate = 1000L * delta / (timestamp - this.prevTimestamp);
                    monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_QUERY_RATE, rate, Long.valueOf(timestamp)));
                }
                this.prevQueries = queries;
                if (this.prevFetches != null) {
                    final long delta = Math.abs(fetches - this.prevFetches);
                    final Long rate = 1000L * delta / (timestamp - this.prevTimestamp);
                    monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_FETCH_RATE, rate, Long.valueOf(timestamp)));
                }
                this.prevFetches = fetches;
                if (this.prevSize != null) {
                    final long delta = Math.abs(size - this.prevSize);
                    final Long rate = 1000L * delta / (timestamp - this.prevTimestamp);
                    monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_SIZE_BYTES_RATE, rate, Long.valueOf(timestamp)));
                }
                this.prevSize = size;
                this.prevTimestamp = timestamp;
                final NodesStatsResponse nsResponse = (NodesStatsResponse)client.admin().cluster().prepareNodesStats(new String[0]).setTransport(true).get();
                if (nsResponse.getNodes().size() > 0) {
                    long txSum = 0L;
                    long rxSum = 0L;
                    for (final NodeStats n2 : nsResponse.getNodes()) {
                        final TransportStats o = n2.getTransport();
                        txSum += o.getTxSize().getBytes();
                        rxSum += o.getRxSize().getBytes();
                    }
                    monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_TX_BYTES, Long.valueOf(txSum), Long.valueOf(timestamp)));
                    monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_RX_BYTES, Long.valueOf(rxSum), Long.valueOf(timestamp)));
                }
                final StringBuilder hotThreads = new StringBuilder();
                final NodesHotThreadsResponse htResponse = (NodesHotThreadsResponse)client.admin().cluster().prepareNodesHotThreads(new String[] { "*" }).get();
                if (htResponse.getNodes().size() > 0) {
                    final boolean first = true;
                    for (final NodeHotThreads n3 : htResponse.getNodes()) {
                        if (!first) {
                            hotThreads.append("\n\n");
                        }
                        final String name = n3.getNode().getName();
                        hotThreads.append(name).append(":\n");
                        hotThreads.append(n3.getHotThreads());
                    }
                    if (hotThreads.length() > 0) {
                        monEvs.add(new MonitorEvent(MonitorModel.ES_SERVER_UUID, MonitorModel.ES_ENTITY_UUID, MonitorEvent.Type.ES_HOT_THREADS, hotThreads.toString(), Long.valueOf(timestamp)));
                    }
                }
            }
            final MonitorBatchEvent batchEvent = new MonitorBatchEvent(timestamp, monEvs);
            MonitorModel.processBatch(batchEvent);
        }
        catch (Exception e) {
            Logger.getLogger("Monitor").error((Object)"ElasticsearchMonitor could not send batch to MonitorModel", (Throwable)e);
        }
    }
}
