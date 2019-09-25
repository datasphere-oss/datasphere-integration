package com.datasphere.TypeHandler;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datasphere.exception.UnsupportedMapping;

public class TargetTypeHandler
{
    private static Logger logger;
    Map<String, TypeHandler> typeHandlerMap;
    
    public TargetTypeHandler() {
        this.typeHandlerMap = new HashMap<String, TypeHandler>();
    }
    
    public int getSqlType(final int sqlType, final int precision) {
        return sqlType;
    }
    
    public void addToMap(final TypeHandler handler) {
        final String key = handler.toString();
        final TypeHandler tHandler = this.typeHandlerMap.get(key);
        if (tHandler != null && TargetTypeHandler.logger.isDebugEnabled()) {
            TargetTypeHandler.logger.debug("Overwriting handler of {" + key + "} with {" + handler.getClass().toString() + "}");
        }
        this.typeHandlerMap.put(key, handler);
    }
    
    public TypeHandler getHandler(final int sqlType, final Class<?> sourceType) throws UnsupportedMapping {
        String key = sqlType + TypeHandler.SEPARATOR + sourceType.getName();
        TypeHandler handler = this.typeHandlerMap.get(key);
        if (handler == null) {
            key = 0 + TypeHandler.SEPARATOR + sourceType.getName();
            handler = this.typeHandlerMap.get(key);
            if (handler == null) {
                final String expMsg = "No matched handler found for SqlType {" + sqlType + "} DataType {" + sourceType + "}";
                TargetTypeHandler.logger.error(expMsg);
                throw new UnsupportedMapping(sqlType, sourceType.getName());
            }
            TargetTypeHandler.logger.info("Could not find appropriate handler for SqlType {" + sqlType + "} DataType {" + sourceType + "}, using {" + handler.getClass().getName() + "} to handle it");
        }
        return handler;
    }
    
    public void initialize() {
        this.addToMap(new IntegerHandler());
        this.addToMap(new DefaultIntegerHandler());
        this.addToMap(new ShortHandler());
        this.addToMap(new DefaultShortHandler());
        this.addToMap(new ShortToIntegerHandler());
        this.addToMap(new ByteToIntegerHandler());
        this.addToMap(new IntegerToDecimalObject());
        this.addToMap(new StringHandler());
        this.addToMap(new DefaultStringHandler());
        this.addToMap(new StringToDecimalHandler());
        this.addToMap(new StringToFloatHandler());
        this.addToMap(new StringToVarBinaryHandler());
        this.addToMap(new ByteArrayHandler());
        this.addToMap(new ByteHandler());
        this.addToMap(new DefaultByteHandler());
        this.addToMap(new StringToBinaryHandler());
        this.addToMap(new ByteArrayToVarBinaryHandler());
        this.addToMap(new ByteToVarBinaryHandler());
        this.addToMap(new DoubleHandler());
        this.addToMap(new DefaultDoubleHandler());
        this.addToMap(new FloatHandler());
        this.addToMap(new DefaultFloatHandler());
        this.addToMap(new LongHandler());
        this.addToMap(new DefaultLongHandler());
        this.addToMap(new DateHandler());
        this.addToMap(new TimestampHandler());
        this.addToMap(new LocalDateHandler());
        this.addToMap(new DefaultLocalDateHandler());
        this.addToMap(new LocalDateToTimestampHandler());
        this.addToMap(new LocalTimeHandler());
        this.addToMap(new DateTimeToTimeHandler());
        this.addToMap(new DefaultLocalTimeHandler());
        this.addToMap(new DateTimeToTimestampHandler());
        this.addToMap(new DateTimeHandler());
        this.addToMap(new DefaultDateTimeHandler());
        this.addToMap(new DateTimeType12Handler());
		this.addToMap(new BigDecimalHandler());
    }
    
    static {
        TargetTypeHandler.logger = LoggerFactory.getLogger((Class)TargetTypeHandler.class);
    }
}
