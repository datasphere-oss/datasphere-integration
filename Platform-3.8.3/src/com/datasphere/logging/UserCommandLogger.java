package com.datasphere.logging;

import org.apache.log4j.*;

public class UserCommandLogger
{
    public static final String COMMAND_LOGGER = "command_logger";
    private static Logger logger;
    private static final String SEPARATOR = " # ";
    
    public static void logCmd(final String userid, final String sessionid, final String msg) {
        if (UserCommandLogger.logger.isDebugEnabled()) {
            UserCommandLogger.logger.debug((Object)(userid + " # " + sessionid + " # " + msg));
        }
    }
    
    static {
        UserCommandLogger.logger = Logger.getLogger("command_logger");
    }
}
