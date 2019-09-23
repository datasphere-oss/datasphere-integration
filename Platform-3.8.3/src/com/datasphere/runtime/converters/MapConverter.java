package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.apache.log4j.*;
import flexjson.*;
import org.eclipse.persistence.sessions.*;
import java.util.*;
import com.datasphere.runtime.utils.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class MapConverter implements Converter
{
    private static Logger logger;
    public static final long serialVersionUID = 2740988897547957681L;
    JSONSerializer ser;
    JSONDeserializer deser;
    
    public MapConverter() {
        this.ser = new JSONSerializer();
        this.deser = new JSONDeserializer();
    }
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue == null) {
            return objectValue;
        }
        synchronized (this.ser) {
            try {
                return this.ser.deepSerialize(objectValue);
            }
            catch (Exception e) {
                MapConverter.logger.error((Object)"Problem serializing", (Throwable)e);
                return null;
            }
        }
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String) {
            try {
                final Map<String, Object> tmpMap = (Map<String, Object>)this.deser.deserialize((String)dataValue);
                final Map<String, Object> retMap = Factory.makeCaseInsensitiveMap();
                retMap.putAll(tmpMap);
                return retMap;
            }
            catch (Exception e) {
                MapConverter.logger.error((Object)"Problem deserializing", (Throwable)e);
                return null;
            }
        }
        return null;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping databaseMapping, final Session session) {
    }
    
    static {
        MapConverter.logger = Logger.getLogger((Class)MapConverter.class);
    }
}
