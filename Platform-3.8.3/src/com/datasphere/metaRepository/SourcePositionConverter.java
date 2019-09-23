package com.datasphere.metaRepository;

import org.apache.log4j.*;
import com.fasterxml.jackson.core.type.*;
import com.datasphere.recovery.*;
import org.eclipse.persistence.sessions.*;
import javax.xml.bind.*;
import java.io.*;
import org.eclipse.persistence.core.sessions.*;

public class SourcePositionConverter extends PropertyDefConverter
{
    private static Logger logger;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<SourcePosition>() {};
    }
    
    @Override
    public Object convertObjectValueToDataValue(final Object objectValue, final Session session) {
        final String dataBaseName = System.getProperty("com.datasphere.config.metaDataRepositoryDB");
        if (objectValue == null) {
            return objectValue;
        }
        if (objectValue instanceof SourcePosition) {
            final ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b;
            try {
                final ObjectOutputStream oos = new ObjectOutputStream(bos);
                oos.writeObject(objectValue);
                oos.close();
                b = bos.toByteArray();
                if (dataBaseName.equalsIgnoreCase("postgres")) {
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
    
    @Override
    public Object convertDataValueToObjectValue(final Object dataValue, final Session session) {
        if (dataValue == null) {
            return null;
        }
        ByteArrayInputStream bis = null;
        if (dataValue instanceof byte[]) {
            bis = new ByteArrayInputStream((byte[])dataValue);
        }
        else {
            final byte[] array = DatatypeConverter.parseBase64Binary((String)dataValue);
            bis = new ByteArrayInputStream(array);
        }
        return this.convertByteArrayToSourcePosition(bis);
    }
    
    private SourcePosition convertByteArrayToSourcePosition(final ByteArrayInputStream bis) {
        SourcePosition sourcePosition;
        try {
            final ObjectInputStream ois = new ObjectInputStream(bis);
            sourcePosition = (SourcePosition)ois.readObject();
        }
        catch (ClassNotFoundException e) {
            sourcePosition = null;
        }
        catch (IOException e2) {
            sourcePosition = null;
        }
        return sourcePosition;
    }
    
    static {
        SourcePositionConverter.logger = Logger.getLogger((Class)SourcePositionConverter.class);
    }
}
