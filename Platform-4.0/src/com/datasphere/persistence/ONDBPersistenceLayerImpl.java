package com.datasphere.persistence;

import com.datasphere.classloading.*;
import com.datasphere.intf.*;

import org.apache.log4j.*;
import org.apache.commons.lang.*;

import java.io.*;
import com.datasphere.recovery.*;
import com.datasphere.hd.*;
import java.util.concurrent.*;
import oracle.kv.*;
import java.util.*;

public class ONDBPersistenceLayerImpl implements PersistenceLayer
{
    private static Logger logger;
    public Map<String, Object> prop;
    public String storeName;
    public int MAX_CONN;
    public String recovery_key;
    private String userName;
    private String password;
    private KVStore kvstore;
    private String url;
    private String kvschema;
    private KVStoreConfig config;
    
    public ONDBPersistenceLayerImpl() {
        this.prop = null;
        this.storeName = null;
        this.MAX_CONN = 0;
        this.recovery_key = null;
        this.userName = null;
        this.password = null;
        this.kvstore = null;
        this.url = null;
        this.kvschema = "kvstore";
        this.config = null;
    }
    
    public ONDBPersistenceLayerImpl(final String kvStoreName, final Map<String, Object> properties) {
        this.prop = null;
        this.storeName = null;
        this.MAX_CONN = 0;
        this.recovery_key = null;
        this.userName = null;
        this.password = null;
        this.kvstore = null;
        this.url = null;
        this.kvschema = "kvstore";
        this.config = null;
        this.storeName = kvStoreName;
        this.recovery_key = this.storeName + "_recovery";
        this.prop = properties;
    }
    
    @Override
    public void init() {
        if (this.prop == null) {
            ONDBPersistenceLayerImpl.logger.error((Object)"No properties are set. Set required properties for ONDB");
            return;
        }
        if (this.prop.get("NOSQL_PROPERTY") == null) {
            ONDBPersistenceLayerImpl.logger.error((Object)"Set either NOSQL_PROPERTY with ONDB url. e.g. localhost:5000");
            return;
        }
        this.url = (String)this.prop.get("NOSQL_PROPERTY");
        if (this.storeName == null) {
            ONDBPersistenceLayerImpl.logger.error((Object)"Set storeName. ");
            return;
        }
        if (this.prop.get("DB_NAME") == null) {
            ONDBPersistenceLayerImpl.logger.warn((Object)"kvstore database is not set. taking kvstore as default one. ");
        }
        else {
            this.kvschema = (String)this.prop.get("DB_NAME");
        }
        if (this.prop.get("ONDB_USER") != null) {
            this.userName = (String)this.prop.get("ONDB_USER");
        }
        if (this.prop.get("ONDB_PASSWD") != null) {
            this.password = (String)this.prop.get("ONDB_PASSWD");
        }
        if (this.prop.get("MAX_CONN") != null) {
            final String mc = (String)this.prop.get("MAX_CONN");
            try {
                this.MAX_CONN = Integer.parseInt(mc);
            }
            catch (Exception ex) {}
        }
        this.config = new KVStoreConfig(this.kvschema, new String[] { this.url });
        if (this.kvstore == null) {
            this.kvstore = KVStoreFactory.getStore(this.config);
        }
    }
    
    private KVStore[] getConnectionsForRead(final int num) {
        if (num < 1) {
            return null;
        }
        final KVStore[] kvs = new KVStore[num];
        for (int ii = 0; ii < num; ++ii) {
            kvs[ii] = KVStoreFactory.getStore(this.config);
        }
        return kvs;
    }
    
    @Override
    public void init(final String storeName) {
        this.init();
    }
    
    @Override
    public int delete(final Object object) {
        throw new NotImplementedException();
    }
    
    @Override
    public Range[] persist(final Object object) {
        int numWritten = 0;
        final Range[] rangeArray = { null };
        final int batchsize = 1000;
        if (object == null) {
            throw new IllegalArgumentException("Cannot persist null object");
        }
        if (object instanceof List) {
            if (ONDBPersistenceLayerImpl.logger.isDebugEnabled()) {
                ONDBPersistenceLayerImpl.logger.debug((Object)("Number of hds to persist :" + ((List)object).size()));
            }
            final OperationFactory of = this.kvstore.getOperationFactory();
            ArrayList<Operation> opList = new ArrayList<Operation>();
            HD trackWacPos = null;
            int ii = 0;
            for (final Object o1 : (List)object) {
                if (ii < batchsize) {
                    final HD hd = trackWacPos = (HD)o1;
                    final String mapKey = hd.getMapKeyString();
                    final long ts = hd.getHDTs();
                    final List<String> minorPath = Arrays.asList(mapKey, String.valueOf(ts));
                    final Key key = Key.createKey(this.storeName, (List)minorPath);
                    final Value value = Value.createValue(serializeObj(hd));
                    opList.add(of.createPut(key, value));
                    ++ii;
                    ++numWritten;
                }
                else {
                    try {
                        this.kvstore.execute((List)opList);
                    }
                    catch (OperationExecutionException | FaultException ex3) {
                        ONDBPersistenceLayerImpl.logger.error((Object)"error doing bulk insert to ONDB database", (Throwable)ex3);
                    }
                    ii = 0;
                    opList = new ArrayList<Operation>();
                }
            }
            if (opList.size() > 0) {
                try {
                    this.kvstore.execute((List)opList);
                }
                catch (OperationExecutionException | FaultException ex4) {
                    ONDBPersistenceLayerImpl.logger.error((Object)"error doing bulk insert to ONDB database", (Throwable)ex4);
                }
            }
        }
        else {
            this.insert((HD)object);
            ++numWritten;
        }
        (rangeArray[0] = new Range(0, numWritten)).setSuccessful(true);
        return rangeArray;
    }
    
    private void insert(final HD hd) {
        final String mapKey = hd.getMapKeyString();
        final long ts = hd.getHDTs();
        final List<String> minorPath = Arrays.asList(mapKey, String.valueOf(ts));
        final Key key = Key.createKey(this.storeName, (List)minorPath);
        final Value value = Value.createValue(serializeObj(hd));
        this.kvstore.put(key, value);
    }
    
    private static byte[] serializeObj(final Object object) {
        byte[] bytes = null;
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = null;
        try {
            out = new ObjectOutputStream(bos);
            out.writeObject(object);
            bytes = bos.toByteArray();
        }
        catch (IOException iex) {
            ONDBPersistenceLayerImpl.logger.error((Object)"exception serializing object:", (Throwable)iex);
        }
        finally {
            try {
                if (out != null) {
                    out.close();
                }
                if (bos != null) {
                    bos.close();
                }
            }
            catch (IOException ex) {}
        }
        return bytes;
    }
    
    private static Object deSerizlizeObj(final byte[] bytes) {
        Thread.currentThread().setContextClassLoader(HDLoader.get());
        final ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
        ObjectInput in = null;
        Object object = null;
        try {
            in = new ObjectInputStream(bis) {
                @Override
                protected Class<?> resolveClass(final ObjectStreamClass desc) throws IOException, ClassNotFoundException {
                    try {
                        return super.resolveClass(desc);
                    }
                    catch (ClassNotFoundException cx) {
                        if (ONDBPersistenceLayerImpl.logger.isTraceEnabled()) {
                            ONDBPersistenceLayerImpl.logger.trace((Object)("** runtime class :" + desc.getName()));
                        }
                        return ClassLoader.getSystemClassLoader().loadClass(desc.getName());
                    }
                }
            };
            object = in.readObject();
        }
        catch (IOException ex) {}
        catch (ClassNotFoundException iex) {
            ONDBPersistenceLayerImpl.logger.error((Object)"exception de-serializing bytes:", (Throwable)iex);
        }
        finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (bis != null) {
                    bis.close();
                }
            }
            catch (IOException ex2) {}
        }
        return object;
    }
    
    @Override
    public void merge(final Object object) {
        ONDBPersistenceLayerImpl.logger.warn((Object)"merge on ONDB is not implemented");
    }
    
    @Override
    public void setStoreName(final String storeName, final String tableName) {
        this.storeName = storeName;
    }
    
    @Override
    public Object get(final Class<?> objectClass, final Object objectId) {
        return null;
    }
    
    @Override
    public List<?> runQuery(final String query, final Map<String, Object> params, final Integer maxResults) {
        return null;
    }
    
    @Override
    public Object runNativeQuery(final String query) {
        return null;
    }
    
    @Override
    public int executeUpdate(final String query, final Map<String, Object> params) {
        return 0;
    }
    
    @Override
    public void close() {
        if (this.kvstore != null) {
            this.kvstore.close();
        }
    }
    
    @Override
    public Position getWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for ONDBPersistenceLayerImpl");
    }
    
    @Override
    public boolean clearWSPosition(final String namespaceName, final String hdStoreName) {
        throw new NotImplementedException("This method is not supported for ONDBPersistenceLayerImpl");
    }
    
    @Override
    public <T extends HD> List<T> getResults(final Class<T> objectClass, final Map<String, Object> filter, final Set<HDKey> excludeKeys) {
        if (ONDBPersistenceLayerImpl.logger.isDebugEnabled()) {
            ONDBPersistenceLayerImpl.logger.debug((Object)("trying to get results from table :" + this.storeName + ", and create objects of type : " + objectClass.getCanonicalName()));
        }
        List<T> results = null;
        long startTime = 0L;
        long endTime = 0L;
        String filterKey = null;
        boolean cacheonly = false;
        if (ONDBPersistenceLayerImpl.logger.isTraceEnabled()) {
            ONDBPersistenceLayerImpl.logger.trace((Object)("filters : " + filter));
        }
        if (filter != null && filter.size() > 0) {
            final Object startTimeVal = filter.get("startTime");
            if (startTimeVal != null) {
                startTime = (long)(Object)new Double(startTimeVal.toString());
                startTime *= 1000L;
            }
            final Object endTimeVal = filter.get("endTime");
            if (endTimeVal != null) {
                endTime = (long)(Object)new Double(endTimeVal.toString());
                endTime *= 1000L;
            }
            filterKey = (String)filter.get("key");
            final Object ocfilter = filter.get("cacheOnly");
            String cfilter = "false";
            if (ocfilter != null) {
                cfilter = ocfilter.toString();
            }
            if (cfilter != null) {
                cacheonly = cfilter.equalsIgnoreCase("true");
            }
        }
        if (ONDBPersistenceLayerImpl.logger.isDebugEnabled()) {
            ONDBPersistenceLayerImpl.logger.debug((Object)("filter:" + filter + ", cacheOnly:" + cacheonly));
        }
        if (cacheonly) {
            return new ArrayList<T>();
        }
        if (startTime > 0L && endTime > 0L) {
            final Key key = Key.createKey(this.storeName);
            final SortedSet<Key> set = (SortedSet<Key>)this.kvstore.multiGetKeys(key, (KeyRange)null, (Depth)null);
            final Set<Key> tobeQueried = new HashSet<Key>();
            for (final Key k1 : set) {
                final String ts = k1.getMinorPath().get(1);
                final long tsLong = Long.parseLong(ts);
                if (tsLong >= startTime && tsLong <= endTime) {
                    tobeQueried.add(k1);
                }
            }
            final List<Key> tobeQueriedList = new ArrayList<Key>();
            tobeQueriedList.addAll(tobeQueried);
            final int totalReads = tobeQueried.size();
            results = new ArrayList<T>(totalReads);
            if (this.MAX_CONN > 1 && totalReads > 0) {
                final int numThreads = this.MAX_CONN - 1;
                final int querySize = totalReads / numThreads;
                final ONDBQuery<T>[] fetchTask = (ONDBQuery<T>[])new ONDBQuery[numThreads];
                final Thread[] t = new Thread[numThreads];
                final CountDownLatch ctl = new CountDownLatch(numThreads);
                int fromIndex = 0;
                int toIndex = querySize - 1;
                if (toIndex < 0) {
                    toIndex = 0;
                }
                for (int ii = 0; ii < numThreads; ++ii) {
                    fetchTask[ii] = new ONDBQuery<T>(tobeQueriedList.subList(fromIndex, toIndex), ctl);
                    fromIndex = toIndex + 1;
                    toIndex = fromIndex + querySize - 1;
                    if (toIndex >= totalReads - 1) {
                        toIndex = totalReads - 1;
                    }
                    (t[ii] = new Thread(fetchTask[ii], "ONDBQuery@" + this.storeName)).start();
                }
                try {
                    ctl.await();
                }
                catch (InterruptedException iex) {
                    ONDBPersistenceLayerImpl.logger.warn((Object)("hd read thread was interrupted due to " + iex.getLocalizedMessage()));
                }
                for (int ii = 0; ii < numThreads; ++ii) {
                    if (fetchTask[ii].getResults() != null) {
                        results.addAll((Collection<? extends T>)fetchTask[ii].getResults());
                    }
                }
            }
            else {
                for (final Key k2 : tobeQueried) {
                    final byte[] valueBytes = this.kvstore.get(k2).getValue().getValue();
                    final T hd = (T)deSerizlizeObj(valueBytes);
                    results.add(hd);
                }
            }
        }
        else if (filterKey != null) {
            final Key key = Key.createKey(this.storeName);
            final SortedSet<Key> set = (SortedSet<Key>)this.kvstore.multiGetKeys(key, (KeyRange)null, (Depth)null);
            final Set<Key> tobeQueried = new HashSet<Key>();
            for (final Key k1 : set) {
                final String wkey = k1.getMinorPath().get(0);
                if (filterKey.equals(wkey)) {
                    tobeQueried.add(k1);
                }
            }
            results = new ArrayList<T>(tobeQueried.size());
            for (final Key k1 : tobeQueried) {
                final byte[] valueBytes2 = this.kvstore.get(k1).getValue().getValue();
                final T hd2 = (T)deSerizlizeObj(valueBytes2);
                results.add(hd2);
            }
        }
        else {
            results = new ArrayList<T>();
            final Key key = Key.createKey(this.storeName);
            final SortedMap<Key, ValueVersion> kvResult = (SortedMap<Key, ValueVersion>)this.kvstore.multiGet(key, (KeyRange)null, (Depth)null);
            for (final Map.Entry<Key, ValueVersion> entry : kvResult.entrySet()) {
                final byte[] valueBytes3 = entry.getValue().getValue().getValue();
                final T hd3 = (T)deSerizlizeObj(valueBytes3);
                results.add(hd3);
            }
        }
        if (ONDBPersistenceLayerImpl.logger.isDebugEnabled()) {
            ONDBPersistenceLayerImpl.logger.debug((Object)("no of results : " + results.size()));
        }
        return results;
    }
    
    @Override
    public <T extends HD> List<T> getResults(final Class<T> objectClass, final String hdKey, final Map<String, Object> filter) {
        if (ONDBPersistenceLayerImpl.logger.isDebugEnabled()) {
            ONDBPersistenceLayerImpl.logger.debug((Object)("trying to get results for store :" + this.storeName + ", and create objects of type : " + objectClass.getCanonicalName() + ", with key :" + hdKey));
        }
        final List<T> results = new ArrayList<T>();
        final Key key = Key.createKey(this.storeName);
        final SortedSet<Key> set = (SortedSet<Key>)this.kvstore.multiGetKeys(key, (KeyRange)null, (Depth)null);
        Key keyLookingFor = null;
        for (final Key k1 : set) {
            final String wkey = k1.getMinorPath().get(0);
            if (hdKey.equals(wkey)) {
                keyLookingFor = k1;
                break;
            }
        }
        if (keyLookingFor != null) {
            final byte[] valueBytes = this.kvstore.get(keyLookingFor).getValue().getValue();
            final T hd = (T)deSerizlizeObj(valueBytes);
            results.add(hd);
        }
        return results;
    }
    
    @Override
    public HStore getHDStore() {
        return null;
    }
    
    @Override
    public void setHDStore(final HStore ws) {
    }
    
    static {
        ONDBPersistenceLayerImpl.logger = Logger.getLogger((Class)ONDBPersistenceLayerImpl.class);
    }
    
    private class ONDBQuery<T> implements Runnable
    {
        List<Key> keys;
        List<T> results;
        CountDownLatch cLatch;
        
        ONDBQuery(final List<Key> keys, final CountDownLatch ctl) {
            this.keys = null;
            this.results = null;
            this.cLatch = null;
            this.keys = keys;
            this.cLatch = ctl;
            this.results = new ArrayList<T>(keys.size());
        }
        
        @Override
        public void run() {
            for (final Key key : this.keys) {
                final byte[] valueBytes = ONDBPersistenceLayerImpl.this.kvstore.get(key).getValue().getValue();
                final T hd = (T)deSerizlizeObj(valueBytes);
                this.results.add(hd);
            }
            this.cLatch.countDown();
        }
        
        public List<T> getResults() {
            return this.results;
        }
    }
}
