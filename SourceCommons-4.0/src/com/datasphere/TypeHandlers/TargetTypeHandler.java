package com.datasphere.TypeHandlers;

import org.apache.log4j.*;
import java.util.*;

import com.datasphere.Exceptions.*;
import com.datasphere.source.lib.meta.*;

public class TargetTypeHandler
{
    private static Logger logger;
    Map<String, TypeHandler> typeHandlerMap;
    public String targetType;
    
    public TargetTypeHandler(final String targetType) {
        this.typeHandlerMap = new HashMap<String, TypeHandler>();
        this.targetType = targetType;
    }
    
    public int getSqlType(final int sqlType, final DatabaseColumn columnDetails) {
        return sqlType;
    }
    
    public void addToMap(final TypeHandler handler) {
        final String key = handler.toString();
        final TypeHandler tHandler = this.typeHandlerMap.get(key);
        if (tHandler != null && TargetTypeHandler.logger.isDebugEnabled()) {
            TargetTypeHandler.logger.debug((Object)("Overwriting handler of {" + key + "} with {" + handler.getClass().toString() + "}"));
        }
        this.typeHandlerMap.put(key, handler);
    }
    
    public TypeHandler getHandler(final int sqlType, final Class<?> sourceType) throws UnsupportedMappingException {
        String key = sqlType + TypeHandler.SEPARATOR + sourceType.getName();
        TypeHandler handler = this.typeHandlerMap.get(key);
        if (handler == null) {
            key = 0 + TypeHandler.SEPARATOR + sourceType.getName();
            handler = this.typeHandlerMap.get(key);
            if (handler == null) {
                final String expMsg = "No matched handler found for SqlType {" + sqlType + "} DataType {" + sourceType + "}";
                TargetTypeHandler.logger.error((Object)expMsg);
                throw new UnsupportedMappingException(sqlType, sourceType.getName());
            }
            try {
                final TypeHandler newHandler = (TypeHandler)handler.clone();
                newHandler.sqlType = sqlType;
                handler = newHandler;
            }
            catch (CloneNotSupportedException e) {
                throw new UnsupportedMappingException("\"couldnot clone for default type handler {\"+key +\"}\"", e);
            }
            TargetTypeHandler.logger.info((Object)("Could not find appropriate handler for SqlType {" + sqlType + "} DataType {" + sourceType + "}, using {" + handler.getClass().getName() + "} to handle it"));
        }
        return handler;
    }
    
    public void initialize() {
        this.addToMap(new IntegerHandler(this.targetType));
        this.addToMap(new DefaultIntegerHandler(this.targetType));
        this.addToMap(new ShortHandler(this.targetType));
        this.addToMap(new DefaultShortHandler(this.targetType));
        this.addToMap(new ShortToIntegerHandler(this.targetType));
        this.addToMap(new ByteToIntegerHandler(this.targetType));
        this.addToMap(new IntegerToDecimalObject(this.targetType));
        this.addToMap(new StringHandler(this.targetType));
        this.addToMap(new DefaultStringHandler(this.targetType));
        this.addToMap(new StringToDecimalHandler(this.targetType));
        this.addToMap(new StringToFloatHandler(this.targetType));
        this.addToMap(new StringToVarBinaryHandler(this.targetType));
        this.addToMap(new ByteArrayHandler(this.targetType));
        this.addToMap(new ByteHandler(this.targetType));
        this.addToMap(new DefaultByteHandler(this.targetType));
        this.addToMap(new ByteArrayToVarBinaryHandler(this.targetType));
        this.addToMap(new ByteArrayToLongVarBinaryHandler(this.targetType));
        this.addToMap(new ByteToVarBinaryHandler(this.targetType));
        this.addToMap(new DoubleHandler(this.targetType));
        this.addToMap(new DefaultDoubleHandler(this.targetType));
        this.addToMap(new FloatHandler(this.targetType));
        this.addToMap(new DefaultFloatHandler(this.targetType));
        this.addToMap(new LongHandler(this.targetType));
        this.addToMap(new DefaultLongHandler(this.targetType));
        this.addToMap(new DateHandler(this.targetType));
        this.addToMap(new TimestampHandler(this.targetType));
        this.addToMap(new LocalDateHandler(this.targetType));
        this.addToMap(new DefaultLocalDateHandler(this.targetType));
        this.addToMap(new LocalDateToTimestampHandler(this.targetType));
        this.addToMap(new LocalTimeHandler(this.targetType));
        this.addToMap(new DateTimeToTimeHandler(this.targetType));
        this.addToMap(new DefaultLocalTimeHandler(this.targetType));
        this.addToMap(new DateTimeToTimestampHandler(this.targetType));
        this.addToMap(new DateTimeHandler(this.targetType));
        this.addToMap(new DefaultDateTimeHandler(this.targetType));
        this.addToMap(new HexStringToBinaryHandler(this.targetType));
        this.addToMap(new HexStringToVarBinary(this.targetType));
        this.addToMap(new HexStringToLongVarBinary(this.targetType));
        this.addToMap(new BooleanToIntegerHandler(this.targetType));
        this.addToMap(new BooleanToBitHandler(this.targetType));
        this.addToMap(new DefaultBooleanHandler(this.targetType));
        this.addToMap(new StringToVarCharHandler(this.targetType));
        this.addToMap(new StringToIntegerHandler(this.targetType));
        this.addToMap(new StringToTinyIntHandler(this.targetType));
        this.addToMap(new StringToShortHandler(this.targetType));
        this.addToMap(new StringToBigIntHandler(this.targetType));
        this.addToMap(new StringToDoubleHandler(this.targetType));
        this.addToMap(new StringToBooHandler(this.targetType));
        this.addToMap(new DateTimeToStringHandler(this.targetType));
        this.addToMap(new BigIntToVarCharHandler(this.targetType));
        this.addToMap(new DoubleToFloatHandler(this.targetType));
        this.addToMap(new IntergerToLongHandler(this.targetType));
    }
    
    static {
        TargetTypeHandler.logger = Logger.getLogger((Class)TargetTypeHandler.class);
    }
}
