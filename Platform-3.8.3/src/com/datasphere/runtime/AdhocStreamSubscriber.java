package com.datasphere.runtime;

import org.apache.log4j.Logger;

import com.datasphere.distribution.HQueue;
import com.datasphere.event.QueryResultEvent;
import com.datasphere.runtime.components.Subscriber;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.uuid.UUID;

public class AdhocStreamSubscriber implements Subscriber
{
    private static Logger logger;
    private final String[] fieldsInfo;
    private final UUID queryMetaObjectUUID;
    private final String queueName;
    private final HQueue consoleQueue;
    private final UUID uuid;
    
    public AdhocStreamSubscriber(final String[] fieldsInfo, final UUID queryMetaObjectUUID, final String queueName, final HQueue consoleQueue) {
        this.fieldsInfo = fieldsInfo;
        this.queryMetaObjectUUID = queryMetaObjectUUID;
        this.queueName = queueName;
        this.consoleQueue = consoleQueue;
        this.uuid = new UUID(System.currentTimeMillis());
    }
    
    public UUID getUuid() {
        return this.uuid;
    }
    
    public String getName() {
        return this.queueName;
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        for (final DARecord wa : event.batch()) {
            final QueryResultEvent e = (QueryResultEvent)wa.data;
            e.setFieldsInfo(this.fieldsInfo);
        }
        event.setQueryID(this.queryMetaObjectUUID);
        if (AdhocStreamSubscriber.logger.isDebugEnabled()) {
            AdhocStreamSubscriber.logger.debug((Object)("Query Sending " + event.batch().size() + " # of objects to WAQueue " + this.queueName));
        }
        this.consoleQueue.put(event);
    }
    
    static {
        AdhocStreamSubscriber.logger = Logger.getLogger((Class)AdhocStreamSubscriber.class);
    }
}
