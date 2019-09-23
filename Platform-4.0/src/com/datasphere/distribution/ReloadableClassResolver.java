package com.datasphere.distribution;

import java.util.concurrent.*;
import com.esotericsoftware.kryo.util.*;
import com.esotericsoftware.kryo.io.*;
import com.datasphere.metaRepository.*;
import com.esotericsoftware.kryo.*;
import com.hazelcast.core.*;
import java.util.*;

public class ReloadableClassResolver implements ClassResolver
{
    public static final byte NAME = -1;
    protected Kryo kryo;
    protected Map<Integer, Registration> idToRegistration;
    protected Map<Class<?>, Registration> classToRegistration;
    protected Map<String, Integer> classNameToId;
    protected Map<String, Class<?>> nameToClass;
    private Registration[] memoizedClassIdArray;
    
    public ReloadableClassResolver() {
        this.kryo = null;
        this.idToRegistration = new ConcurrentHashMap<Integer, Registration>();
        this.classToRegistration = new ConcurrentHashMap<Class<?>, Registration>();
        this.classNameToId = new ConcurrentHashMap<String, Integer>();
        this.nameToClass = new ConcurrentHashMap<String, Class<?>>();
        this.memoizedClassIdArray = new Registration[65536];
    }
    
    public void setKryo(final Kryo kryo) {
        if (this.kryo == null) {
            this.kryo = kryo;
        }
    }
    
    public void removeClassRegistration(final Class<?> clazz) {
        final Registration r = this.classToRegistration.get(clazz);
        if (r != null) {
            this.classToRegistration.remove(clazz);
        }
        if (r != null) {
            if (r.getId() != -1) {
                final Registration rId = this.idToRegistration.get(r.getId());
                if (rId != null) {
                    this.idToRegistration.remove(r.getId());
                }
            }
            if (r.getId() >= 0 && r.getId() < this.memoizedClassIdArray.length) {
                this.memoizedClassIdArray[r.getId()] = null;
            }
        }
        final Class<?> clazzName = this.nameToClass.get(Util.className((Class)clazz));
        if (clazzName != null) {
            this.nameToClass.remove(Util.className((Class)clazz));
        }
    }
    
    public Registration register(final Registration registration) {
        if (registration == null) {
            throw new IllegalArgumentException("registration cannot be null.");
        }
        this.classToRegistration.put(registration.getType(), registration);
        if (registration.getId() != -1) {
            this.idToRegistration.put(registration.getId(), registration);
        }
        this.classNameToId.put(Util.className(registration.getType()), registration.getId());
        if (registration.getType().isPrimitive()) {
            this.classToRegistration.put(Util.getWrapperClass(registration.getType()), registration);
        }
        return registration;
    }
    
    public Registration registerImplicit(final Class type) {
        return this.register(new Registration(type, this.kryo.getDefaultSerializer(type), -1));
    }
    
    public Registration getRegistration(final Class type) {
        return this.classToRegistration.get(type);
    }
    
    public Registration getRegistration(final int classID) {
        return this.idToRegistration.get(classID);
    }
    
    public Registration writeClass(final Output output, final Class type) {
        if (type == null) {
            output.writeByte((byte)0);
            return null;
        }
        Registration r = this.classToRegistration.get(type);
        if (r == null) {
            synchronized (this.kryo) {
                r = this.kryo.getRegistration(type);
                this.classToRegistration.put(type, r);
            }
        }
        if (r.getId() == -1) {
            this.writeName(output, type, r);
        }
        else {
            output.writeInt(r.getId() + 2, true);
        }
        return r;
    }
    
    protected void writeName(final Output output, final Class<?> type, final Registration registration) {
        output.writeByte(1);
        output.write(0);
        output.writeString(type.getName());
    }
    
    public Registration readClass(final Input input) {
        final int classID = input.readInt(true);
        Registration r = null;
        if (classID == 0) {
            return null;
        }
        if (classID == 1) {
            r = this.readName(input);
        }
        else {
            final int regId = classID - 2;
            if (regId >= 0 && regId < this.memoizedClassIdArray.length && this.memoizedClassIdArray[regId] != null) {
                r = this.memoizedClassIdArray[regId];
            }
            if (r == null) {
                r = this.idToRegistration.get(regId);
                if (r == null) {
                    String className = null;
                    final IMap<String, Integer> idMap = HazelcastSingleton.get().getMap("#classIds");
                    for (final Map.Entry<String, Integer> entry : idMap.entrySet()) {
                        if (entry.getValue() == regId && !entry.getKey().equals("#allIds")) {
                            className = entry.getKey();
                            break;
                        }
                    }
                    if (className == null) {
                        throw new KryoException("Encountered unregistered class ID: " + regId);
                    }
                    try {
                        synchronized (this.kryo) {
                            final Class<?> clazz = this.kryo.getClassLoader().loadClass(className);
                            r = this.kryo.register((Class)clazz, regId);
                        }
                    }
                    catch (ClassNotFoundException cnfe) {
                        throw new KryoException("Encountered unregistered class ID with no matching synthetic class: " + regId, (Throwable)cnfe);
                    }
                }
                if (regId >= 0 && regId < this.memoizedClassIdArray.length) {
                    this.memoizedClassIdArray[regId] = r;
                }
            }
        }
        return r;
    }
    
    protected Registration readName(final Input input) {
        input.readInt(true);
        Class<?> type = null;
        final String className = input.readString();
        type = this.nameToClass.get(className);
        if (type == null) {
            try {
                type = this.kryo.getClassLoader().loadClass(className);
            }
            catch (ClassNotFoundException ex) {
                throw new KryoException("Unable to find class: " + className, (Throwable)ex);
            }
            this.nameToClass.put(className, type);
        }
        Registration r = this.classToRegistration.get(type);
        if (r == null) {
            synchronized (this.kryo) {
                r = this.kryo.getRegistration((Class)type);
                this.classToRegistration.put(type, r);
            }
        }
        return r;
    }
    
    public void reset() {
        if (!this.kryo.isRegistrationRequired()) {}
    }
    
    public void dumpClassIds() {
        System.out.println("**** ALL KRYO REGISTERED CLASSES ****");
        for (final Map.Entry<Class<?>, Registration> entry : this.classToRegistration.entrySet()) {
            System.out.println("**  " + entry.getKey().getName() + " - " + entry.getValue().getId() + " - " + entry.getValue().getSerializer().getClass().getName());
        }
        System.out.println("*************************************");
        System.out.println("**** ALL KRYO REGISTERED CLASSES BY ID****");
        for (final Map.Entry<String, Integer> entry2 : this.classNameToId.entrySet()) {
            System.out.println("**  " + entry2.getValue() + " - " + entry2.getKey());
        }
        System.out.println("*************************************");
    }
}
