package com.datasphere.runtime;

import org.apache.log4j.*;
import com.datasphere.uuid.*;

public class ShowStreamManager
{
    private static ShowStreamManager INSTANCE;
    private static Logger logger;
    public static final UUID ShowStreamUUID;
    
    public static ShowStreamManager getInstance() {
        return ShowStreamManager.INSTANCE;
    }
    
    static {
        ShowStreamManager.INSTANCE = new ShowStreamManager();
        ShowStreamManager.logger = Logger.getLogger((Class)ShowStreamManager.class);
        ShowStreamUUID = new UUID("3624C7DG-2292-4535-BBE5-E376C5F5BC42");
    }
}
