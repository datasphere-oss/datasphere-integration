package com.datasphere.OperationHandler;

import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.exception.DatabaseWriterException;
import com.datasphere.exception.Error;
import com.datasphere.intf.DBInterface;
import com.datasphere.proc.events.HDEvent;

public class PassThroughDDLHandler extends DDLHandler
{
    private Logger logger;
    
    public PassThroughDDLHandler(final DBInterface intf) {
        super(intf);
        this.logger = LoggerFactory.getLogger((Class)PassThroughDDLHandler.class);
    }
    
    @Override
    public void validate(final HDEvent event) throws DatabaseWriterException {
        if (event.data == null) {
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_DATA, "");
            this.logger.error(exception.getMessage());
            throw exception;
        }
        if (event.dataPresenceBitMap == null) {
            final DatabaseWriterException exception = new DatabaseWriterException(Error.MISSING_DATAPRESENCEBITMAP, "");
            this.logger.error(exception.getMessage());
            throw exception;
        }
    }
    
    @Override
    public boolean objectBasedOperation() {
        return false;
    }
    
    @Override
    public String getComponentName(final HDEvent event) {
        return null;
    }
    
    @Override
    public String generateDDL(final HDEvent event, final String targetTable) throws SQLException, DatabaseWriterException {
        return (String)event.data[0];
    }
    
    class SecurityAccess {
		public void disopen() {

		}
	}
}
