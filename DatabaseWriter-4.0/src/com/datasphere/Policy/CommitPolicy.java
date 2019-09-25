package com.datasphere.Policy;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.recovery.Position;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.databasewriter.EventData;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public class CommitPolicy extends Policy
{
    private Logger logger;
    int eventCount;
    int interval;
    boolean closeCalled;
    Timer timer;
    private static int DEFAULT_EVENT_COUNT;
    private static int DEFAULT_INTERVAL;
    
    public CommitPolicy(final Property prop, final DBInterface intf, final PolicyCallback callback) {
        super(intf, callback);
        this.logger = LoggerFactory.getLogger((Class)CommitPolicy.class);
        this.closeCalled = false;
        this.init(prop);
    }
    
    public CommitPolicy(final Property prop, final Policy link) {
        super(prop, link);
        this.logger = LoggerFactory.getLogger((Class)CommitPolicy.class);
        this.closeCalled = false;
        this.init(prop);
    }
    
    private void init(final Property prop) {
        this.thresholdEventCount = prop.getInt("EventCount", CommitPolicy.DEFAULT_EVENT_COUNT);
        this.interval = prop.getInt("Interval", CommitPolicy.DEFAULT_INTERVAL);
        if (this.interval > 0) {
            if (this.logger.isInfoEnabled()) {
                this.logger.info("Setting up timer for CommitPolicy {Interval : " + this.interval + "} seconds");
            }
            (this.timer = new Timer("CommitTimer")).scheduleAtFixedRate(this, this.interval * 1000, this.interval * 1000);
        }
        if (this.thresholdEventCount == 0 && this.interval == 0) {
            if (this.logger.isInfoEnabled()) {
                this.logger.info("CommitPolicy is disabled");
            }
            this.isDisabled = true;
        }
        this.closeCalled = false;
    }
    
    @Override
    public void commit() throws SQLException, DatabaseWriterException {
        if (this.isDisabled || (this.thresholdEventCount > 0 && this.eventCount + 1 > this.thresholdEventCount)) {
            this.commitBufferedTxn();
        }
    }
    
    private void commitBufferedTxn() throws SQLException, DatabaseWriterException {
        this.dbInterface.commit();
        this.reset();
    }
    
    @Override
    public int execute(final LinkedList<EventData> events) throws SQLException, DatabaseWriterException {
        final int retCnt = super.execute(events);
        this.eventCount += retCnt;
        return retCnt;
    }
    
    @Override
    public void reset() {
        if (this.logger.isDebugEnabled()) {
            this.logger.debug("Resetting CommitPolicy.");
        }
        this.eventCount = 0;
    }
    
    @Override
    public void close() throws SQLException, DatabaseWriterException {
        this.commit();
        if (this.timer != null) {
            this.timer.cancel();
            this.timer.purge();
        }
        this.commitBufferedTxn();
        super.close();
    }
    
    @Override
    public void run() {
        this.logger.debug("Commit timer expired, issuing commit.");
        if (!this.closeCalled) {
            try {
                this.commitBufferedTxn();
            }
            catch (Exception e) {
                this.callback.onError(null, e);
            }
        }
        else {
            this.logger.info("Policy.close() is called, not executing commit");
        }
    }
    
    @Override
    public void add(final HDEvent event, final Position pos, final String target) throws SQLException, DatabaseWriterException {
    }
    
    @Override
    public void validate(final Property prop) {
        int thresholdEventCount = prop.getInt("EventCount", CommitPolicy.DEFAULT_EVENT_COUNT);
        final int interval = prop.getInt("Interval", CommitPolicy.DEFAULT_INTERVAL);
        if (thresholdEventCount <= 0 && interval <= 0) {
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("CommitPolicy is disabled");
            }
            thresholdEventCount = 1;
            this.isDisabled = true;
        }
    }
    
    static {
        CommitPolicy.DEFAULT_EVENT_COUNT = 0;
        CommitPolicy.DEFAULT_INTERVAL = 0;
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
