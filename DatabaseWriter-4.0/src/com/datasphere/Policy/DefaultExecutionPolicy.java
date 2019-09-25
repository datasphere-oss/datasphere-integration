package com.datasphere.Policy;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.LinkedList;

import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.OperationHandler.OperationHandler;
import com.datasphere.databasewriter.EventData;
import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.proc.events.HDEvent;

public class DefaultExecutionPolicy extends ExecutionPolicy
{
    private Logger logger;
    
    public DefaultExecutionPolicy(final PolicyCallback callbackImpl) {
        super(callbackImpl);
        this.logger = LoggerFactory.getLogger((Class)DefaultExecutionPolicy.class);
    }
    
    @Override
    public int execute(final OperationHandler handler, final String fullyQualifiedName, final String query, final PreparedStatement stmt, final LinkedList<EventData> events) throws SQLException, DatabaseWriterException {
        int startEventOffset = 0;
        int noOfBatchedEvent = 0;
        int rowsAffected = 0;
        do {
            noOfBatchedEvent = 0;
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Starting position {" + startEventOffset + "} total no of events {" + events.size() + "}");
            }
            for (int itr = startEventOffset; itr < events.size(); ++itr) {
                handler.bind(fullyQualifiedName, (HDEvent)events.get(itr).event, stmt);
                stmt.addBatch();
                ++noOfBatchedEvent;
            }
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("{" + noOfBatchedEvent + "} batched for execution");
            }
            try {
                final long startTime = System.currentTimeMillis();
                final int[] result = stmt.executeBatch();
                final long endTime = System.currentTimeMillis();
                this.lastBatchExecuteTime = DateTime.now();
                this.executionLatency = endTime - startTime;
                int failedEventIdx = -1;
                for (int itr2 = 0; itr2 < result.length; ++itr2) {
                    if (result[itr2] > 0 || result[itr2] == -2) {
                        ++rowsAffected;
                        if (events.get(startEventOffset + itr2).position != null) {
                            this.callbackImpl.updateConsolidatedPosition(events.get(startEventOffset + itr2).position);
                        }
                    }
                    else if (result[itr2] == -3) {
                        failedEventIdx = itr2;
                        this.causedEvent = events.get(startEventOffset + itr2).event;
                        this.logger.warn("Execution failed for {" + events.get(startEventOffset + itr2).event + "}");
                        break;
                    }
                }
                if (failedEventIdx != -1) {
                    startEventOffset += failedEventIdx + 1;
                }
                else {
                    startEventOffset += result.length;
                }
            }
            catch (BatchUpdateException batchException) {
	            	if(logger.isDebugEnabled()) {
	                this.logger.error("Some of the event in batched failed. Query {" + query + "}");
	            	}
                int[] result2 = batchException.getUpdateCounts();
                if (result2.length > 0) {
                    for (int itr3 = 0; itr3 < result2.length; ++itr3) {
                        if (result2[itr3] > 0 || result2[itr3] == -2) {
                            ++rowsAffected;
                            if (events.get(startEventOffset + itr3).position != null) {
                                this.callbackImpl.updateConsolidatedPosition(events.get(startEventOffset + itr3).position);
                            }
                        }
                        else if (result2[itr3] == -3) {}
                    }
                    startEventOffset += result2.length - 1;
                }
                else {
                    this.causedEvent = events.get(startEventOffset).event;
                }
                this.causedEvent = events.get(startEventOffset).event;
                if (!this.callbackImpl.onError(this.causedEvent, batchException)) {
                    return rowsAffected;
                }
                if (events.get(startEventOffset).position != null) {
                    this.callbackImpl.updateConsolidatedPosition(events.get(startEventOffset).position);
                }
                ++startEventOffset;
                throw new SQLException(batchException);
            }
        } while (startEventOffset + 1 < events.size());
        return rowsAffected;
    }
    
    class SecurityAccess {
	    public void disopen() {
	        
	    }
	}
}
