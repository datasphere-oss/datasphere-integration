package com.datasphere.metaRepository;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import com.datasphere.runtime.meta.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class UserTypeEnumConverter implements Converter
{
    private static final long serialVersionUID = 5863685664514502453L;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue instanceof MetaInfo.User.AUTHORIZATION_TYPE) {
            return ((MetaInfo.User.AUTHORIZATION_TYPE)objectValue).toString();
        }
        return null;
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof String) {
            if (!((String)dataValue).isEmpty()) {
                if (((String)dataValue).equalsIgnoreCase(MetaInfo.User.AUTHORIZATION_TYPE.LDAP.toString())) {
                    return MetaInfo.User.AUTHORIZATION_TYPE.LDAP;
                }
                return MetaInfo.User.AUTHORIZATION_TYPE.INTERNAL;
            }
        }
        else if (dataValue instanceof MetaInfo.User.AUTHORIZATION_TYPE) {
            return dataValue;
        }
        return MetaInfo.User.AUTHORIZATION_TYPE.INTERNAL;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping mapping, final Session session) {
    }
}
