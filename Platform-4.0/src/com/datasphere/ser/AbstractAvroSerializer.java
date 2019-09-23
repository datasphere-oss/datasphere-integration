package com.datasphere.ser;

import org.apache.avro.*;
import com.esotericsoftware.kryo.*;
import org.apache.avro.generic.*;
import com.esotericsoftware.kryo.io.*;
import java.io.*;
import org.apache.avro.io.*;
import java.util.*;

public class AbstractAvroSerializer extends Serializer<GenericContainer>
{
    static Map<String, Schema> schemaCache;
    static Map<String, Decoder> decoderCache;
    static Map<String, GenericDatumReader> genericDatumReaderCache;
    
    public void write(final Kryo kryo, final Output output, final GenericContainer record) {
        final String fingerprint = record.getSchema().toString();
        output.writeString(fingerprint);
        final GenericDatumWriter<GenericContainer> writer = (GenericDatumWriter<GenericContainer>)new GenericDatumWriter(record.getSchema());
        final BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder((OutputStream)output, (BinaryEncoder)null);
        try {
            writer.write(record, (Encoder)encoder);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public GenericContainer read(final Kryo kryo, final Input input, final Class<GenericContainer> aClass) {
        final String schemaString = input.readString();
        Decoder decoder = null;
        Schema theSchema = AbstractAvroSerializer.schemaCache.get(schemaString);
        if (theSchema == null) {
            theSchema = new Schema.Parser().parse(schemaString);
            AbstractAvroSerializer.schemaCache.put(schemaString, theSchema);
        }
        GenericDatumReader<GenericContainer> reader = (GenericDatumReader<GenericContainer>)AbstractAvroSerializer.genericDatumReaderCache.get(schemaString);
        if (reader == null) {
            reader = (GenericDatumReader<GenericContainer>)new GenericDatumReader(theSchema);
            AbstractAvroSerializer.genericDatumReaderCache.put(schemaString, reader);
        }
        decoder = AbstractAvroSerializer.decoderCache.get(schemaString);
        if (decoder == null) {
            decoder = (Decoder)DecoderFactory.get().directBinaryDecoder((InputStream)input, (BinaryDecoder)null);
            AbstractAvroSerializer.decoderCache.put(schemaString, decoder);
        }
        else {
            decoder = (Decoder)DecoderFactory.get().directBinaryDecoder((InputStream)input, (BinaryDecoder)decoder);
        }
        GenericContainer foo;
        try {
            foo = (GenericContainer)reader.read(null, decoder);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return foo;
    }
    
    static {
        AbstractAvroSerializer.schemaCache = new HashMap<String, Schema>();
        AbstractAvroSerializer.decoderCache = new HashMap<String, Decoder>();
        AbstractAvroSerializer.genericDatumReaderCache = new HashMap<String, GenericDatumReader>();
    }
}
