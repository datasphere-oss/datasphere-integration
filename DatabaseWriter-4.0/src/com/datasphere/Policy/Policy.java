package com.datasphere.Policy;

import java.sql.SQLException;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;
import com.datasphere.recovery.Position;
import com.datasphere.source.lib.prop.Property;
import com.datasphere.databasewriter.EventData;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public abstract class Policy extends TimerTask
{
    LinkedList<EventData> eventList;
    DBInterface dbInterface;
    Policy linkedPolicy;
    PolicyCallback callback;
    int eventCount;
    int thresholdEventCount;
    boolean isDisabled;
    private static TreeMap<String, String> ddlOperationMap;
    private Logger logger;
    public static int DEFAULT_EVENT_COUNT;
    public static int DEFAULT_INTERVAL;
    public static String EVENT_COUNT;
    public static String INTERVAL;
    
    public Policy(final DBInterface intf, final PolicyCallback callback) {
        this.logger = LoggerFactory.getLogger((Class)Policy.class);
        this.dbInterface = intf;
        this.callback = callback;
    }
    
    public Policy(final Property prop, final DBInterface intf, final PolicyCallback callback) {
        this.logger = LoggerFactory.getLogger((Class)Policy.class);
        this.dbInterface = intf;
        this.init(prop);
        this.callback = callback;
    }
    
    public Policy(final Property prop, final Policy link) {
        this.logger = LoggerFactory.getLogger((Class)Policy.class);
        this.linkedPolicy = link;
        this.callback = link.callback;
        this.init(prop);
    }
    
    private void init(final Property prop) {
        this.thresholdEventCount = prop.getInt("EventCount", Policy.DEFAULT_EVENT_COUNT);
        this.eventList = new LinkedList<EventData>();
    }
    
    public Policy(final Policy link) {
        this.logger = LoggerFactory.getLogger((Class)Policy.class);
        this.linkedPolicy = link;
        this.callback = link.callback;
    }
    
    public abstract void validate(final Property p0);
    
    public abstract void add(final HDEvent p0, final Position p1, final String p2) throws SQLException, DatabaseWriterException;
    
    protected int eventCount() {
        if (this.linkedPolicy != null) {
            return this.linkedPolicy.eventCount();
        }
        return this.eventCount;
    }
    
    public synchronized int onDDL(final EventData ddl) throws SQLException, DatabaseWriterException {
        int recCnt = 0;
        if (this.linkedPolicy != null) {
            recCnt = this.execute(this.eventList);
            this.commit();
            recCnt = this.linkedPolicy.onDDL(ddl);
        }
        else {
            this.dbInterface.onDDL(ddl);
        }
        return recCnt;
    }
    
    public int execute(final LinkedList<EventData> events) throws SQLException, DatabaseWriterException {
        int recCnt = 0;
        if (this.linkedPolicy != null) {
            recCnt = this.linkedPolicy.execute(events);
        }
        else {
            recCnt = this.dbInterface.execute(events);
        }
        return recCnt;
    }
    
    public void reset() {
        if (this.linkedPolicy != null) {
            this.linkedPolicy.reset();
        }
    }
    
    public void clear() {
        if (this.linkedPolicy != null) {
            this.linkedPolicy.clear();
        }
        else {
            this.eventCount = 0;
        }
    }
    
    public boolean canAdd(final HDEvent event, final String target) {
        return true;
    }
    
    public void commit() throws SQLException, DatabaseWriterException {
        if (this.linkedPolicy != null) {
            this.linkedPolicy.commit();
        }
    }
    
    @Override
    public abstract void run();
    
    public void close() throws SQLException, DatabaseWriterException {
        if (this.linkedPolicy != null) {
            this.linkedPolicy.close();
        }
        else {
            this.dbInterface.close();
        }
    }
    
    public boolean isDDLOperation(final HDEvent event) {
        final String operation = (String)event.metadata.get(Constant.OPERATION);
        return !Policy.ddlOperationMap.containsKey(operation);
    }
    
    static {
        Policy.ddlOperationMap = new TreeMap<String, String>(String.CASE_INSENSITIVE_ORDER) {
            {
                this.put(Constant.INSERT_OPERATION, Constant.INSERT_OPERATION);
                this.put(Constant.UPDATE_OPERATION, Constant.UPDATE_OPERATION);
                this.put(Constant.DELETE_OPERATION, Constant.DELETE_OPERATION);
                this.put(Constant.SELECT_OPERATION, Constant.SELECT_OPERATION);
            }
        };
        Policy.DEFAULT_EVENT_COUNT = 1000;
        Policy.DEFAULT_INTERVAL = 60;
        Policy.EVENT_COUNT = "EventCount";
        Policy.INTERVAL = "Interval";
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
