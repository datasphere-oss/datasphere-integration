package com.datasphere.exceptionhandling;

import java.util.Queue;

import org.apache.log4j.Logger;

import com.datasphere.runtime.ExceptionEvent;
import com.datasphere.runtime.components.Subscriber;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.DARecord;

import com.datasphere.uuid.UUID;

public class HExceptionMgr implements Subscriber
{
    private static Logger logger;
    public static final UUID ExceptionStreamUUID;
    private static volatile HExceptionMgr instance;
    private static final int LIMIT = 1000;
    private Queue<ExceptionEvent> exceptions;
    
    private HExceptionMgr() {
        this.exceptions = new FixedSizeQueue<ExceptionEvent>(1000);
    }
    
    public static HExceptionMgr get() {
        if (HExceptionMgr.instance == null) {
            synchronized (HExceptionMgr.class) {
                if (HExceptionMgr.instance == null) {
                    HExceptionMgr.instance = new HExceptionMgr();
                }
            }
        }
        return HExceptionMgr.instance;
    }
    
    public String getName() {
        return "Global.ExceptionManager";
    }
    
    @Override
    public void receive(final Object linkID, final ITaskEvent event) throws Exception {
        if (HExceptionMgr.logger.isInfoEnabled()) {
            HExceptionMgr.logger.info((Object)"received an exception object.");
        }
        synchronized (this.exceptions) {
            final IBatch<DARecord> ees = (IBatch<DARecord>)event.batch();
            if (ees != null && ees.size() > 0) {
                for (final DARecord we : ees) {
                    final Object ee = we.data;
                    if (ee instanceof ExceptionEvent) {
                        this.exceptions.add((ExceptionEvent)ee);
                    }
                }
            }
        }
    }
    
    static {
        HExceptionMgr.logger = Logger.getLogger((Class)HExceptionMgr.class);
        ExceptionStreamUUID = new UUID("5619C7DF-2292-4535-BBE5-E376C5F5BC42");
        HExceptionMgr.instance = null;
    }
}
