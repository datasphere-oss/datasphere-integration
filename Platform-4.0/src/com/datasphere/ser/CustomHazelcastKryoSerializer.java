package com.datasphere.ser;

import com.hazelcast.nio.serialization.*;
import com.esotericsoftware.kryo.*;
import com.hazelcast.nio.*;
import java.io.*;
import com.esotericsoftware.kryo.io.*;

public class CustomHazelcastKryoSerializer implements StreamSerializer<Object>
{
    private final Class clazz;
    private final int identifier;
    
    public CustomHazelcastKryoSerializer(final Class clazzParam, final int id) {
        this.clazz = clazzParam;
        this.identifier = id;
    }
    
    public int getTypeId() {
        return this.identifier;
    }
    
    public void destroy() {
    }
    
    public void write(final ObjectDataOutput out, final Object object) throws IOException {
        final Kryo kryo = KryoSerializer.getSerializer();
        final OutputChunked output = new OutputChunked((OutputStream)out, 4096);
        kryo.writeObject((Output)output, object);
        output.endChunks();
        output.flush();
    }
    
    public Object read(final ObjectDataInput inputStream) throws IOException {
        final InputStream in = (InputStream)inputStream;
        final InputChunked input = new InputChunked(in, 4096);
        final Kryo kryo = KryoSerializer.getSerializer();
        return kryo.readObject((Input)input, this.clazz);
    }
}
