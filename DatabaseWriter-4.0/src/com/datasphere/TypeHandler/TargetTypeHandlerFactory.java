package com.datasphere.TypeHandler;

import java.util.Map;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.common.constants.Constant;

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
                    handler = new OracleTypeHandler();
                }
                else if (Constant.MYSQL_TYPE.equalsIgnoreCase(targetType)) {
                    handler = new MySQLTypeHandler();
                }
                else if (Constant.MSSQL_TYPE.equalsIgnoreCase(targetType)) {
                    handler = new MSSQLTypeHandler();
                }
                else if (Constant.POSTGRESS_TYPE.equalsIgnoreCase(targetType) || Constant.EDB_TYPE.equalsIgnoreCase(targetType)) {
                    handler = new PostgressHandler();
                }
                else {
                    TargetTypeHandlerFactory.logger.warn("Using default type handler for {" + targetType + "}");
                    handler = new TargetTypeHandler();
                }
                handler.initialize();
                TargetTypeHandlerFactory.targetTypeHandlerMap.put(targetType, handler);
            }
        }
        return handler;
    }
    
    static {
        TargetTypeHandlerFactory.logger = LoggerFactory.getLogger((Class)TargetTypeHandlerFactory.class);
        TargetTypeHandlerFactory.targetTypeHandlerMap = new TreeMap<String, TargetTypeHandler>(String.CASE_INSENSITIVE_ORDER);
    }
}
