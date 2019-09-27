package com.datasphere.source.avro.codec;

import org.apache.avro.file.*;
import java.util.zip.*;
import java.nio.*;
import org.xerial.snappy.*;
import java.io.*;

public class SnappyCodec extends Codec
{
    private CRC32 crc32;
    
    public SnappyCodec() {
        this.crc32 = new CRC32();
    }
    
    public String getName() {
        return "snappy";
    }
    
    public ByteBuffer compress(final ByteBuffer in) throws IOException {
        final ByteBuffer out = ByteBuffer.allocate(Snappy.maxCompressedLength(in.remaining()) + 4);
        final int size = Snappy.compress(in.array(), in.position(), in.remaining(), out.array(), 0);
        this.crc32.reset();
        this.crc32.update(in.array(), in.position(), in.remaining());
        out.putInt(size, (int)this.crc32.getValue());
        out.limit(size + 4);
        return out;
    }
    
    public ByteBuffer decompress(final ByteBuffer in) throws IOException {
        final ByteBuffer out = ByteBuffer.allocate(Snappy.uncompressedLength(in.array(), in.position(), in.remaining() - 4));
        final int size = Snappy.uncompress(in.array(), in.position(), in.remaining() - 4, out.array(), 0);
        out.limit(size);
        this.crc32.reset();
        this.crc32.update(out.array(), 0, size);
        if (in.getInt(in.limit() - 4) != (int)this.crc32.getValue()) {
            throw new IOException("Checksum failure");
        }
        return out;
    }
    
    public int hashCode() {
        return this.getName().hashCode();
    }
    
    public boolean equals(final Object obj) {
        return this == obj || this.getClass() == obj.getClass();
    }
}
