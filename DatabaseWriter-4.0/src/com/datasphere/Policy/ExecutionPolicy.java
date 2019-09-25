package com.datasphere.Policy;

import com.datasphere.event.*;
import org.joda.time.*;

import java.util.*;
import java.sql.*;

import com.datasphere.source.lib.prop.*;
import com.datasphere.common.constants.*;
import com.datasphere.source.lib.utils.*;
import com.datasphere.OperationHandler.*;
import com.datasphere.databasewriter.*;
import com.datasphere.exception.*;

public abstract class ExecutionPolicy
{
    long executionLatency;
    PolicyCallback callbackImpl;
    Event causedEvent;
    DateTime lastBatchExecuteTime;
    int rowsAffected;
    
    public ExecutionPolicy(final PolicyCallback callbackImpl) {
        this.callbackImpl = callbackImpl;
    }
    
    public abstract int execute(final OperationHandler p0, final String p1, final String p2, final PreparedStatement p3, final LinkedList<EventData> p4) throws SQLException, DatabaseWriterException;
    
    public long executionLatency() {
        return this.executionLatency;
    }
    
    public Event causedEvent() {
        return this.causedEvent;
    }
    
    public DateTime lastBatchExecuteTime() {
        return this.lastBatchExecuteTime;
    }
    
    public static ExecutionPolicy createPolicy(final Property prop, final PolicyCallback callback) {
        final String dbURL = prop.getString(Constant.CONNECTION_URL, (String)null);
        final String dbType = Utils.getDBType(dbURL);
        if (Constant.EDB_TYPE.equalsIgnoreCase(dbType) || Constant.POSTGRESS_TYPE.equalsIgnoreCase(dbType)) {
            return new PostgresExecutionPolicy(callback);
        }
        return new DefaultExecutionPolicy(callback);
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
