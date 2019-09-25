package com.datasphere.proc;

import java.sql.SQLException;
import java.util.Map;
import java.util.NoSuchElementException;

import com.datasphere.intf.SourceMetadataProvider;
import com.datasphere.uuid.UUID;
import com.datasphere.proc.events.HDEvent;

public interface DatabaseReader extends SourceMetadataProvider
{
	void initDR(final Map<String, Object> p0) throws Exception;
    
    HDEvent nextEvent(final UUID p0) throws NoSuchElementException, SQLException;
    
    String getCurrentTableName();
    
    void close() throws SQLException, Exception;
    
//    void initializeNextSet() throws SQLException;
}
