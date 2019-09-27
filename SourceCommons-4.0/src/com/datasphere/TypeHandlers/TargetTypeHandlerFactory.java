package com.datasphere.TypeHandlers;

import org.apache.log4j.*;
import com.datasphere.common.constants.*;
import java.util.*;

public class TargetTypeHandlerFactory
{
    private static Logger logger;
    private static Map<String, TargetTypeHandler> targetTypeHandlerMap;
    
    public static TargetTypeHandler getTargetTypeHandler(final String targetType) {
        TargetTypeHandler handler = null;
        synchronized (TargetTypeHandlerFactory.targetTypeHandlerMap) {
            handler = TargetTypeHandlerFactory.targetTypeHandlerMap.get(targetType);
            if (handler == null) {
                if (Constant.ORACLE_TYPE.equalsIgnoreCase(targetType)) {
                    handler = new OracleTypeHandler(targetType);
                }
                else if (Constant.MYSQL_TYPE.equalsIgnoreCase(targetType)) {
                    handler = new MySQLTypeHandler(targetType);
                }
                else if (Constant.MSSQL_TYPE.equalsIgnoreCase(targetType)) {
                    handler = new MSSQLTypeHandler(targetType);
                }
                else if (Constant.POSTGRESS_TYPE.equalsIgnoreCase(targetType) || Constant.EDB_TYPE.equalsIgnoreCase(targetType)) {
                    handler = new PostgressHandler(targetType);
                }
                else {
                    TargetTypeHandlerFactory.logger.warn((Object)("Using default type handler for {" + targetType + "}"));
                    handler = new TargetTypeHandler(targetType);
                }
                handler.initialize();
                TargetTypeHandlerFactory.targetTypeHandlerMap.put(targetType, handler);
            }
        }
        return handler;
    }
    
    static {
        TargetTypeHandlerFactory.logger = Logger.getLogger((Class)TargetTypeHandlerFactory.class);
        TargetTypeHandlerFactory.targetTypeHandlerMap = new TreeMap<String, TargetTypeHandler>(String.CASE_INSENSITIVE_ORDER);
    }
}
