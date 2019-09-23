package com.datasphere.metaRepository;

import org.eclipse.persistence.mappings.converters.*;
import org.apache.log4j.*;
import flexjson.*;
import org.eclipse.persistence.sessions.*;
import com.datasphere.runtime.meta.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class MetaInfoStatusConverter implements Converter
{
    private static Logger logger;
    JSONSerializer ser;
    JSONDeserializer deser;
    private static final long serialVersionUID = 1061473287059512670L;
    
    public MetaInfoStatusConverter() {
        this.ser = new JSONSerializer();
        this.deser = new JSONDeserializer();
    }
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue == null) {
            return this.ser.deepSerialize((Object)new MetaInfoStatus());
        }
        synchronized (this.ser) {
            try {
                return this.ser.deepSerialize(objectValue);
            }
            catch (Exception e) {
                MetaInfoStatusConverter.logger.error((Object)"Problem serializing", (Throwable)e);
                return null;
            }
        }
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return new MetaInfoStatus();
        }
        try {
            return this.deser.deserialize((String)dataValue);
        }
        catch (Exception e) {
            MetaInfoStatusConverter.logger.error((Object)"Problem deserializing", (Throwable)e);
            return null;
        }
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping mapping, final Session session) {
    }
    
    static {
        MetaInfoStatusConverter.logger = Logger.getLogger((Class)MetaInfoStatusConverter.class);
    }
}
