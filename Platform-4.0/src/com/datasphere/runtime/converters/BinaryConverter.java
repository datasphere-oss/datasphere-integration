package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import java.io.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class BinaryConverter implements Converter
{
    private static final long serialVersionUID = -6537432266455948270L;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        if (objectValue != null) {
            try {
                final ByteArrayOutputStream bos = new ByteArrayOutputStream();
                final ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(objectValue);
                oos.close();
                return bos.toByteArray();
            }
            catch (IOException ex) {}
        }
        return null;
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue instanceof byte[]) {
            try {
                final ByteArrayInputStream bis = new ByteArrayInputStream((byte[])dataValue);
                final ObjectInputStream ois = new ObjectInputStream(bis);
                return ois.readObject();
            }
            catch (IOException ex) {}
            catch (ClassNotFoundException ex2) {}
        }
        return null;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping databaseMapping, final Session session) {
    }
}
