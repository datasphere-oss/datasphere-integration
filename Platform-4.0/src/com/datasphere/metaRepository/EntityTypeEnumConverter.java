package com.datasphere.metaRepository;

import org.eclipse.persistence.mappings.converters.*;
import org.apache.log4j.*;
import org.eclipse.persistence.sessions.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class EntityTypeEnumConverter implements Converter
{
    private static final long serialVersionUID = 2989187146445709055L;
    private static Logger logger;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue instanceof EntityType) {
            return ((EntityType)objectValue).getValue();
        }
        return EntityType.UNKNOWN.getValue();
    }
    
    public Object convertDataValueToObjectValue(Object dataValue, final Session session) {
        if (dataValue instanceof String) {
            dataValue = Integer.parseInt((String)dataValue);
        }
        final MetaDataDbProvider metaDataDbProvider = BaseServer.getMetaDataDBProviderDetails();
        return metaDataDbProvider.getIntegerValueFromData(dataValue);
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping mapping, final Session session) {
    }
    
    static {
        EntityTypeEnumConverter.logger = Logger.getLogger((Class)EntityTypeEnumConverter.class);
    }
}
