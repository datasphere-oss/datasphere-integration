package com.datasphere.usagemetrics.collector;

import java.io.*;

public class SerializableToByteArray extends ByteArrayOutputStream
{
    public SerializableToByteArray(final Serializable object) {
        final ObjectOutput objectOutput = openObjectOutput(this);
        if (objectOutput != null) {
            writeObjectOutput(objectOutput, object);
            closeObjectOutput(objectOutput);
        }
    }
    
    public static byte[] getBytes(final Serializable object) {
        final SerializableToByteArray serializable = new SerializableToByteArray(object);
        return serializable.toByteArray();
    }
    
    private static ObjectOutput openObjectOutput(final OutputStream outputStream) {
        ObjectOutput result = null;
        try {
            result = new ObjectOutputStream(outputStream);
        }
        catch (IOException ex) {}
        return result;
    }
    
    private static void writeObjectOutput(final ObjectOutput objectOutput, final Serializable object) {
        try {
            objectOutput.writeObject(object);
        }
        catch (IOException ex) {}
    }
    
    private static void closeObjectOutput(final ObjectOutput objectOutput) {
        try {
            objectOutput.close();
        }
        catch (IOException ex) {}
    }
}
