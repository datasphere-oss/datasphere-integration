package com.datasphere.source.avro.codec;

import org.apache.avro.file.*;
import java.nio.*;
import java.io.*;
import java.util.zip.*;

public class DeflateCodec extends Codec
{
    private ByteArrayOutputStream outputBuffer;
    private Deflater deflater;
    private Inflater inflater;
    private boolean nowrap;
    private int compressionLevel;
    
    public DeflateCodec(final int compressionLevel) {
        this.nowrap = true;
        this.compressionLevel = compressionLevel;
    }
    
    public DeflateCodec() {
        this.nowrap = true;
        this.compressionLevel = -1;
    }
    
    public String getName() {
        return "deflate";
    }
    
    public ByteBuffer compress(final ByteBuffer data) throws IOException {
        final ByteArrayOutputStream baos = this.getOutputBuffer(data.remaining());
        final DeflaterOutputStream ios = new DeflaterOutputStream(baos, this.getDeflater());
        this.writeAndClose(data, ios);
        final ByteBuffer result = ByteBuffer.wrap(baos.toByteArray());
        return result;
    }
    
    public ByteBuffer decompress(final ByteBuffer data) throws IOException {
        final ByteArrayOutputStream baos = this.getOutputBuffer(data.remaining());
        final InflaterOutputStream ios = new InflaterOutputStream(baos, this.getInflater());
        this.writeAndClose(data, ios);
        final ByteBuffer result = ByteBuffer.wrap(baos.toByteArray());
        return result;
    }
    
    private void writeAndClose(final ByteBuffer data, final OutputStream to) throws IOException {
        final byte[] input = data.array();
        final int offset = data.arrayOffset() + data.position();
        final int length = data.remaining();
        try {
            to.write(input, offset, length);
        }
        finally {
            to.close();
        }
    }
    
    private Inflater getInflater() {
        if (null == this.inflater) {
            this.inflater = new Inflater(this.nowrap);
        }
        this.inflater.reset();
        return this.inflater;
    }
    
    private Deflater getDeflater() {
        if (null == this.deflater) {
            this.deflater = new Deflater(this.compressionLevel, this.nowrap);
        }
        this.deflater.reset();
        return this.deflater;
    }
    
    private ByteArrayOutputStream getOutputBuffer(final int suggestedLength) {
        if (null == this.outputBuffer) {
            this.outputBuffer = new ByteArrayOutputStream(suggestedLength);
        }
        this.outputBuffer.reset();
        return this.outputBuffer;
    }
    
    public int hashCode() {
        return this.nowrap ? 0 : 1;
    }
    
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (this.getClass() != obj.getClass()) {
            return false;
        }
        final DeflateCodec other = (DeflateCodec)obj;
        return this.nowrap == other.nowrap;
    }
    
    public String toString() {
        return this.getName() + "-" + this.compressionLevel;
    }
}
