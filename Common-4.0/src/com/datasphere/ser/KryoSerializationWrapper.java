package com.datasphere.ser;

import java.io.*;

public class KryoSerializationWrapper implements Serializable
{
    private static final long serialVersionUID = -1585682752066344644L;
    private byte[] s;
    
    public KryoSerializationWrapper(final Object target) {
        this.s = KryoSerializer.write(target);
    }
    
    public Object readResolve() throws ObjectStreamException {
        return KryoSerializer.read(this.s);
    }
    
    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.writeInt(this.s.length);
        out.write(this.s);
    }
    
    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        final int len = in.readInt();
        in.readFully(this.s = new byte[len]);
    }
}
