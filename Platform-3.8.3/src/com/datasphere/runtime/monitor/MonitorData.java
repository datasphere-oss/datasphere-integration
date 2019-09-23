package com.datasphere.runtime.monitor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import com.datasphere.distribution.Partitionable;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.datasphere.uuid.UUID;

public class MonitorData implements Serializable, KryoSerializable
{
    private static final long serialVersionUID = 3204834325566246466L;
    public static int HISTORY_EXPIRE_TIME;
    Key key;
    Map<String, Object> values;
    List<MonitorEvent> history;
    
    public MonitorData() {
    }
    
    public MonitorData(final UUID entityID, final UUID serverID) {
        this.key = new Key(entityID, serverID);
        this.values = new ConcurrentHashMap<String, Object>();
        this.history = new ArrayList<MonitorEvent>();
    }
    
    public Key getKey() {
        return this.key;
    }
    
    public void addValues(final Map<String, Object> vAdd) {
        synchronized (this.values) {
            this.values.putAll(vAdd);
        }
    }
    
    public Map<String, Object> getValues() {
        final Map<String, Object> vRet = new HashMap<String, Object>();
        vRet.putAll(this.values);
        return vRet;
    }
    
    public void addSingleValue(final String k, final Object value) {
        this.values.put(k, value);
    }
    
    public void addSingleEvent(final MonitorEvent event) {
        synchronized (this.history) {
            this.history.add(event);
        }
    }
    
    public void addHistory(final List<MonitorEvent> hAdd) {
        if (!hAdd.isEmpty()) {
            synchronized (this.history) {
                this.history.addAll(hAdd);
                Collections.sort(this.history, new Comparator<MonitorEvent>() {
                    @Override
                    public int compare(final MonitorEvent o1, final MonitorEvent o2) {
                        return Long.compare(o1.timeStamp, o2.timeStamp);
                    }
                });
                if (!this.history.isEmpty()) {
                    final MonitorEvent lastEvent = this.history.get(this.history.size() - 1);
                    final long historyCutoff = lastEvent.timeStamp - MonitorData.HISTORY_EXPIRE_TIME;
                    MonitorEvent candidate = this.history.get(0);
                    while (candidate != null && candidate.timeStamp < historyCutoff) {
                        this.history.remove(0);
                        if (!this.history.isEmpty()) {
                            candidate = this.history.get(0);
                        }
                        else {
                            candidate = null;
                        }
                    }
                }
            }
        }
    }
    
    public void addLatest(final MonitorData latest) {
        this.addValues(latest.values);
        this.addHistory(latest.history);
    }
    
    public List<MonitorEvent> getHistory() {
        final List<MonitorEvent> hRet = new ArrayList<MonitorEvent>();
        synchronized (this.history) {
            hRet.addAll(this.history);
        }
        return hRet;
    }
    
    public void write(final Kryo kryo, final Output output) {
        this.key.write(kryo, output);
        synchronized (this.values) {
            kryo.writeObject(output, (Object)this.values);
        }
        synchronized (this.history) {
            kryo.writeObject(output, (Object)this.history);
        }
    }
    
    public void read(final Kryo kryo, final Input input) {
        (this.key = new Key()).read(kryo, input);
        this.values = (Map<String, Object>)kryo.readObject(input, (Class)HashMap.class);
        this.history = (List<MonitorEvent>)kryo.readObject(input, (Class)ArrayList.class);
    }
    
    static {
        MonitorData.HISTORY_EXPIRE_TIME = 300000;
    }
    
    public static class Key implements Serializable, KryoSerializable, Partitionable
    {
        private static final long serialVersionUID = -8753566499697728394L;
        UUID entityID;
        UUID serverID;
        int hash;
        
        public Key() {
        }
        
        public Key(final UUID entityID, final UUID serverID) {
            this.entityID = entityID;
            this.serverID = serverID;
            this.hash = Objects.hash(serverID, entityID);
        }
        
        public void write(final Kryo kryo, final Output output) {
            this.entityID.write(kryo, output);
            this.serverID.write(kryo, output);
            output.writeInt(this.hash);
        }
        
        public void read(final Kryo kryo, final Input input) {
            (this.entityID = new UUID()).read(kryo, input);
            (this.serverID = new UUID()).read(kryo, input);
            this.hash = input.readInt();
        }
        
        @Override
        public int hashCode() {
            return this.hash;
        }
        
        @Override
        public boolean equals(final Object obj) {
            if (obj instanceof Key) {
                final Key other = (Key)obj;
                return this.entityID.equals((Object)other.entityID) && this.serverID.equals((Object)other.serverID);
            }
            return false;
        }
        
        @Override
        public String toString() {
            return "{ entityID: " + this.entityID.getUUIDString() + ", serverID: " + this.serverID.getUUIDString() + " }";
        }
        
        public boolean usePartitionId() {
            return false;
        }
        
        public Object getPartitionKey() {
            return this.entityID;
        }
        
        public int getPartitionId() {
            return 0;
        }
    }
}
