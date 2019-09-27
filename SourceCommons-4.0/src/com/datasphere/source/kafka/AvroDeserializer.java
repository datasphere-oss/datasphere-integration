package com.datasphere.source.kafka;

import com.datasphere.kafka.schemaregistry.*;
import java.util.*;
import org.apache.avro.generic.*;
import org.apache.avro.*;
import java.nio.*;
import org.apache.avro.io.*;

public class AvroDeserializer
{
    private Map<Integer, DatumReader<GenericRecord>> datumeReaderCache;
    private SchemaRegistry client;
    private final DecoderFactory decoderFactory;
    
    public AvroDeserializer(final String schemaRegistryUrl) {
        this.datumeReaderCache = new HashMap<Integer, DatumReader<GenericRecord>>();
        this.decoderFactory = DecoderFactory.get();
        this.client = new SchemaRegistry(schemaRegistryUrl);
    }
    
    public AvroDeserializer(final SchemaRegistry schemaRegistryClient) {
        this.datumeReaderCache = new HashMap<Integer, DatumReader<GenericRecord>>();
        this.decoderFactory = DecoderFactory.get();
        this.client = schemaRegistryClient;
    }
    
    public DatumReader<GenericRecord> getReader(final int id) throws Exception {
        if (this.datumeReaderCache.containsKey(id)) {
            return this.datumeReaderCache.get(id);
        }
        final Schema schema = this.client.getSchemaById(id);
        this.datumeReaderCache.put(id, (DatumReader<GenericRecord>)new GenericDatumReader(schema));
        return this.datumeReaderCache.get(id);
    }
    
    public GenericRecord deserialize(final byte[] bytes) throws Exception {
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final int dataLength = bytes.length - 4;
        final int id = buffer.getInt();
        final DatumReader<GenericRecord> reader = this.getReader(id);
        final GenericRecord record = (GenericRecord)reader.read((GenericRecord)null, (Decoder)this.decoderFactory.binaryDecoder(buffer.array(), 4, dataLength, (BinaryDecoder)null));
        return record;
    }
    
    public void close() throws Exception {
        if (this.client != null) {
            this.client.close();
            this.client = null;
        }
        this.datumeReaderCache = null;
    }
}

