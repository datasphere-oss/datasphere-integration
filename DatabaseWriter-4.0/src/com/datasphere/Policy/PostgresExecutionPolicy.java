package com.datasphere.Policy;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.LinkedList;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.OperationHandler.OperationHandler;
import com.datasphere.databasewriter.EventData;
import com.datasphere.databasewriter.ExceptionData;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.proc.events.HDEvent;

public class PostgresExecutionPolicy extends ExecutionPolicy
{
    private Logger logger;
    private static String SAVE_POINT_NAME;
    Connection dbConnection;
    Savepoint sp;
    
    public PostgresExecutionPolicy(final PolicyCallback callbackImpl) {
        super(callbackImpl);
        this.logger = LoggerFactory.getLogger((Class)PostgresExecutionPolicy.class);
        this.sp = null;
        this.dbConnection = callbackImpl.getConnection();
    }
    
    @Override
    public int execute(final OperationHandler handler, final String fullyQualifiedName, final String query, final PreparedStatement stmt, final LinkedList<EventData> events) throws SQLException, DatabaseWriterException {
        final int noOfEvents = events.size();
        this.rowsAffected = 0;
        final boolean batchFailed = false;
        this.sp = this.dbConnection.setSavepoint(PostgresExecutionPolicy.SAVE_POINT_NAME);
        for (int itr = 0; itr < noOfEvents; ++itr) {
            handler.bind(fullyQualifiedName, (HDEvent)events.get(itr).event, stmt);
            stmt.addBatch();
        }
        try {
            final long startTime = System.currentTimeMillis();
            final int[] result = stmt.executeBatch();
            final long endTime = System.currentTimeMillis();
            this.lastBatchExecuteTime = DateTime.now();
            this.executionLatency = endTime - startTime;
            for (int itr2 = 0; itr2 < result.length; ++itr2) {
                if (result[itr2] > 0 || result[itr2] == -2) {
                    ++this.rowsAffected;
                    if (events.get(itr2).position != null) {
                        this.callbackImpl.updateConsolidatedPosition(events.get(itr2).position);
                    }
                }
                else if (result[itr2] == -3) {
                    this.causedEvent = events.get(itr2).event;
                    this.logger.warn("Execution failed for {" + events.get(itr2).event + "}");
                    break;
                }
            }
            this.dbConnection.releaseSavepoint(this.sp);
        }
        catch (BatchUpdateException batchException) {
            this.rowsAffected = 0;
            this.dbConnection.rollback(this.sp);
            this.dbConnection.releaseSavepoint(this.sp);
            for (int itr3 = 0; itr3 < noOfEvents; ++itr3) {
                try {
                    this.sp = this.dbConnection.setSavepoint(PostgresExecutionPolicy.SAVE_POINT_NAME);
                    handler.bind(fullyQualifiedName, (HDEvent)events.get(itr3).event, stmt);
                    final int ret = stmt.executeUpdate();
                    if (events.get(itr3).position != null) {
                        this.callbackImpl.updateConsolidatedPosition(events.get(itr3).position);
                    }
                    this.dbConnection.releaseSavepoint(this.sp);
                    ++this.rowsAffected;
                }
                catch (SQLException exp) {
                    this.dbConnection.rollback(this.sp);
                    this.dbConnection.releaseSavepoint(this.sp);
                    this.causedEvent = events.get(itr3).event;
                    final ExceptionData expData = new ExceptionData(exp, (HDEvent)this.causedEvent);
                    if (!this.callbackImpl.onError(this.causedEvent, exp)) {
                        return this.rowsAffected;
                    }
                    if (events.get(itr3).position != null) {
                        this.callbackImpl.updateConsolidatedPosition(events.get(itr3).position);
                    }
                }
            }
        }
        return this.rowsAffected;
    }
    
    static {
        PostgresExecutionPolicy.SAVE_POINT_NAME = "DATAEXCHANGE_BATCH_SAVE_POINT";
    }
    
    class SecurityAccess {
	    public void disopen() {
	        
	    }
	}
}
