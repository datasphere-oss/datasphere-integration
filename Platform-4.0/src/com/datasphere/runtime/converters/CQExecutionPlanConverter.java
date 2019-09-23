package com.datasphere.runtime.converters;

import org.eclipse.persistence.mappings.converters.*;
import org.eclipse.persistence.sessions.*;
import com.datasphere.runtime.meta.*;
import javax.xml.bind.*;
import java.io.*;
import org.eclipse.persistence.mappings.*;
import org.eclipse.persistence.core.mappings.*;
import org.eclipse.persistence.core.sessions.*;

public class CQExecutionPlanConverter implements Converter
{
    public static final long serialVersionUID = 2740988897548956781L;
    
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        final String dataBaseName = System.getProperty("com.datasphere.config.metaDataRepositoryDB");
        if (objectValue == null) {
            return objectValue;
        }
        if (objectValue instanceof CQExecutionPlan) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b;
            try {
                final ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(objectValue);
                oos.close();
                b = bos.toByteArray();
                if (dataBaseName != null && dataBaseName.equalsIgnoreCase("postgres")) {
                    final String resultString = DatatypeConverter.printBase64Binary(b);
                    return resultString;
                }
            }
            catch (IOException e) {
                b = null;
            }
            return b;
        }
        return null;
    }
    
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return dataValue;
        }
        if (dataValue instanceof byte[]) {
            final ByteArrayInputStream bis = new ByteArrayInputStream((byte[])dataValue);
            return this.convertByteArrayToCQP(bis);
        }
        if (dataValue instanceof String) {
            final byte[] array = DatatypeConverter.parseBase64Binary((String)dataValue);
            final ByteArrayInputStream bis2 = new ByteArrayInputStream(array);
            return this.convertByteArrayToCQP(bis2);
        }
        return null;
    }
    
    private CQExecutionPlan convertByteArrayToCQP(final ByteArrayInputStream bis) {
        CQExecutionPlan cqp;
        try {
            final ObjectInputStream ois = new ObjectInputStream(bis);
            cqp = (CQExecutionPlan)ois.readObject();
        }
        catch (ClassNotFoundException e) {
            cqp = null;
        }
        catch (IOException e2) {
            cqp = null;
        }
        return cqp;
    }
    
    public boolean isMutable() {
        return false;
    }
    
    public void initialize(final DatabaseMapping databaseMapping, final Session session) {
    }
}
