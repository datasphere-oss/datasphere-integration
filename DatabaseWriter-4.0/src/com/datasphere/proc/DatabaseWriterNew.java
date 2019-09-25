package com.datasphere.proc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.databasewritermgr.DatabaseWriterMgr;

public class DatabaseWriterNew extends DatabaseWriterMgr
{
    private static Logger logger;
    
    static {
        DatabaseWriterNew.logger = LoggerFactory.getLogger((Class)DatabaseWriterNew.class);
    }
}
