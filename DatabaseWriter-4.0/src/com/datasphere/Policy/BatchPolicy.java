package com.datasphere.Policy;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.event.Event;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.databasewriter.EventData;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public class BatchPolicy extends Policy
{
    private Logger logger;
    int eventCount;
    int interval;
    Timer timer;
    String recentTarget;
    boolean closeCalled;
    boolean isEmpty;
    String tableName;
    String operation;
    byte[] beforeBitMap;
    byte[] dataBitMap;
    String batchTarget;
    private String nullPresence;
    
    public BatchPolicy(final Property prop, final DBInterface intf, final PolicyCallback callback) {
        super(prop, intf, callback);
        this.logger = LoggerFactory.getLogger((Class)BatchPolicy.class);
        this.closeCalled = false;
        this.isEmpty = true;
        this.nullPresence = "";
        this.closeCalled = false;
        this.isEmpty = true;
    }
    
    public BatchPolicy(final Property prop, final Policy link) {
        super(prop, link);
        this.logger = LoggerFactory.getLogger((Class)BatchPolicy.class);
        this.closeCalled = false;
        this.isEmpty = true;
        this.nullPresence = "";
        this.thresholdEventCount = prop.getInt(BatchPolicy.EVENT_COUNT, BatchPolicy.DEFAULT_EVENT_COUNT);
        this.interval = prop.getInt(BatchPolicy.INTERVAL, BatchPolicy.DEFAULT_INTERVAL);
        if (this.interval > 0) {
            if (this.logger.isInfoEnabled()) {
                this.logger.info("Setting up timer for BatchPolicy {Interval : " + this.interval + "} seconds");
            }
            (this.timer = new Timer("BatchTimer")).scheduleAtFixedRate(this, this.interval * 1000, this.interval * 1000);
        }
        if (this.thresholdEventCount == 0 && this.interval == 0) {
            if (this.logger.isInfoEnabled()) {
                this.logger.info("CommitPolicy is disabled");
            }
            this.isDisabled = true;
        }
        this.closeCalled = false;
        this.isEmpty = true;
    }
    
    @Override
    public boolean canAdd(final HDEvent event, final String target) {
        final String opr = (String)event.metadata.get("OperationName");
        if ((this.tableName == null || target.equalsIgnoreCase(this.tableName)) && (this.operation == null || opr.equalsIgnoreCase(this.operation)) && (this.dataBitMap == null || Arrays.equals(event.dataPresenceBitMap, this.dataBitMap)) && (this.beforeBitMap == null || Arrays.equals(event.beforePresenceBitMap, this.beforeBitMap)) && this.hasSimilarNullPattern(event, opr)) {
            if (this.isEmpty) {
                this.isEmpty = false;
                this.tableName = target;
                this.operation = opr;
                this.dataBitMap = event.dataPresenceBitMap;
                this.beforeBitMap = event.beforePresenceBitMap;
            }
            return true;
        }
        this.tableName = target;
        this.operation = opr;
        this.dataBitMap = event.dataPresenceBitMap;
        this.beforeBitMap = event.beforePresenceBitMap;
        return false;
    }
    
    private boolean hasSimilarNullPattern(final HDEvent event, final String operation) {
        if (operation.toLowerCase().equals("insert") || operation.toLowerCase().equals("select")) {
            return true;
        }
        Object[] dataImg;
        if (operation.toLowerCase().equals("delete")) {
            dataImg = event.data;
        }
        else {
            dataImg = event.before;
        }
        String nullPattern = "";
        for (int idx = 0; idx < dataImg.length; ++idx) {
            if (BuiltInFunc.IS_PRESENT(event, dataImg, idx) && dataImg[idx] == null) {
                nullPattern = nullPattern + "" + idx + ",";
            }
        }
        if (nullPattern.equals(this.nullPresence)) {
            return true;
        }
        this.nullPresence = nullPattern;
        return false;
    }
    
    private void addEvent(final HDEvent event, final Position pos, final String target) {
        final EventData eventData = new EventData((Event)event, pos, target);
        final int eventCnt;
        synchronized (this.eventList) {
            this.eventList.add(eventData);
            eventCnt = this.eventList.size();
        }
        if (this.logger.isTraceEnabled()) {
            this.logger.trace("Added event into list. # of events {" + eventCnt + "}");
        }
        this.recentTarget = target;
    }
    
    @Override
    public void reset() {
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Restting BatchPolicy");
        }
        this.isEmpty = true;
    }
    
    @Override
    public void add(final HDEvent event, final Position pos, final String target) throws SQLException, DatabaseWriterException {
    		try {
	        if (this.linkedPolicy != null) {
	            this.linkedPolicy.add(event, pos, target);
	        }
	        if (this.isDDLOperation(event)) {
	            final EventData eventData = new EventData((Event)event, pos, target);
	            this.onDDL(eventData);
	            this.commit();
	            return;
	        }
	        if (this.canAdd(event, target)) {
	            this.addEvent(event, pos, target);
	        }
	        else {
	            this.eventCount += this.execute(this.eventList);
	            this.addEvent(event, pos, target);
	            this.commit();
	        }
	        if (this.isDisabled || (this.thresholdEventCount > 0 && this.eventList.size() >= this.thresholdEventCount)) {
	            this.eventCount += this.execute(this.eventList);
	            this.commit();
	        }
    		}catch(Exception e) {
    			e.printStackTrace();
    			try {
				throw new Exception(e.getMessage());
			} catch (Exception e1) {
				e1.printStackTrace();
			}
    		}
    }
    
    @Override
    public int execute(final LinkedList<EventData> events) throws SQLException, DatabaseWriterException {
        int recCnt = 0;
        final LinkedList<EventData> newList;
        synchronized (events) {
            newList = new LinkedList<EventData>(events);
            events.clear();
        }
        recCnt = super.execute(newList);
        this.reset();
        return recCnt;
    }
    
    @Override
    public void run() {
        this.logger.debug("BatchTimer expired, executing buffered transactions");
        if (!this.closeCalled) {
            if (!this.eventList.isEmpty()) {
                try {
                    final int recordAffected = this.execute(this.eventList);
                    if (this.logger.isDebugEnabled()) {
                        this.logger.debug("BatchPolicy-Timer: Executed batch, no of rows affected {" + recordAffected + "}");
                    }
                    this.commit();
                }
                catch (DatabaseWriterException | SQLException ex2) {
                    this.callback.onError(null, ex2);
                }
            }
            else if (this.logger.isDebugEnabled()) {
                this.logger.debug("BatchPolicy Timer: No event to process");
            }
        }
        else {
            this.logger.info("Policy.close() is called, not executing buffered operations");
        }
    }
    
    @Override
    public void close() throws SQLException, DatabaseWriterException {
        this.closeCalled = true;
        if (this.timer != null) {
            this.timer.cancel();
            this.timer.purge();
        }
        if (!this.eventList.isEmpty()) {
            this.execute(this.eventList);
        }
        super.close();
    }
    
    @Override
    public void validate(final Property prop) {
        int thresholdEventCount = prop.getInt("EventCount", BatchPolicy.DEFAULT_EVENT_COUNT);
        final int interval = prop.getInt("Interval", BatchPolicy.DEFAULT_INTERVAL);
        if (thresholdEventCount <= 0 && interval <= 0) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("BatchPolicy is disabled");
            }
            thresholdEventCount = 1;
            this.isDisabled = true;
        }
    }
    
    class SecurityAccess {
	    public void disopen() {
	        
	    }
	}
}
