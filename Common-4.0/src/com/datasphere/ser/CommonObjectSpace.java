package com.datasphere.ser;

import java.util.Queue;
import java.util.concurrent.ConcurrentMap;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;

import com.datasphere.event.SimpleEvent;
import com.datasphere.jmqmessaging.StreamInfoResponse;
import com.datasphere.jmqmessaging.ZMQReceiverInfo;
import com.datasphere.messaging.Address;
import com.datasphere.messaging.ReceiverInfo;
import com.datasphere.proc.records.PublishableRecord;
import com.datasphere.recovery.Position;
import com.datasphere.runtime.DistLink;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.StreamEvent;
import com.datasphere.runtime.containers.DefaultTaskEvent;
import com.datasphere.runtime.containers.ITaskEvent;
import com.datasphere.runtime.containers.DARecord;
import com.datasphere.uuid.UUID;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class CommonObjectSpace
{
    public static final int COMMON_REGISTRATION_START_INDEX = 50;
    public static final int COMMON_REGISTRATION_MAX_SIZE = 100;
    
    public static void registerClasses(final Kryo kryo) {
        int index = 50;
        kryo.register((Class)byte[].class, index++);
        kryo.register((Class)char[].class, index++);
        kryo.register((Class)short[].class, index++);
        kryo.register((Class)int[].class, index++);
        kryo.register((Class)long[].class, index++);
        kryo.register((Class)float[].class, index++);
        kryo.register((Class)double[].class, index++);
        kryo.register((Class)boolean[].class, index++);
        kryo.register((Class)String[].class, index++);
        kryo.register((Class)Object[].class, index++);
        kryo.register((Class)ConcurrentMap.class, index++);
        kryo.register((Class)Queue.class, index++);
        kryo.register((Class)UUID.class, index++);
        kryo.register((Class)DistLink.class, index++);
        kryo.register((Class)ITaskEvent.class, index++);
        kryo.register((Class)DefaultTaskEvent.class, index++);
        kryo.register((Class)DARecord.class, index++);
        kryo.register((Class)StreamInfoResponse.class, index++);
        kryo.register((Class)ZMQReceiverInfo.class, index++);
        kryo.register((Class)ReceiverInfo.class, index++);
        kryo.register((Class)Address.class, index++);
        kryo.register((Class)PublishableRecord.class, index++);
        kryo.register((Class)StreamEvent.class, index++);
        kryo.register((Class)com.datasphere.proc.records.Record.class, index++);
        kryo.register((Class)SimpleEvent.class, index++);
        kryo.register((Class)Position.class, index++);
        kryo.register((Class)RecordKey.class, index++);
        kryo.register((Class)ISOChronology.class, (Serializer)new Serializer<ISOChronology>() {
            DateTimeZone defaultZone = DateTimeZone.getDefault();
            
            public ISOChronology read(final Kryo kr, final Input input, final Class<ISOChronology> type) {
                final String zoneID = input.readString();
                if (zoneID == null) {
                    return ISOChronology.getInstance();
                }
                final DateTimeZone zone = DateTimeZone.forID(zoneID);
                return ISOChronology.getInstance(zone);
            }
            
            public void write(final Kryo kr, final Output output, final ISOChronology object) {
                final DateTimeZone zone = object.getZone();
                if (zone != this.defaultZone) {
                    final String zoneID = zone.getID();
                    output.writeString(zoneID);
                }
                else {
                    output.writeString((String)null);
                }
            }
        });
        kryo.register((Class)DateTime.class, (Serializer)new Serializer<DateTime>() {
            DateTimeZone defaultZone = DateTimeZone.getDefault();
            
            public DateTime read(final Kryo kr, final Input input, final Class<DateTime> type) {
                final long instant = input.readLong();
                Chronology cr = null;
                final String zoneID = input.readString();
                if (zoneID != null) {
                    final DateTimeZone zone = DateTimeZone.forID(zoneID);
                    cr = (Chronology)ISOChronology.getInstance(zone);
                }
                return new DateTime(instant, cr);
            }
            
            public void write(final Kryo kr, final Output output, final DateTime object) {
                output.writeLong(object.getMillis());
                final DateTimeZone zone = object.getZone();
                if (zone != this.defaultZone) {
                    final String zoneID = zone.getID();
                    output.writeString(zoneID);
                }
                else {
                    output.writeString((String)null);
                }
            }
        });
    }
}
