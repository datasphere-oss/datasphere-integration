package com.datasphere.runtime.containers;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Logger;

import com.datasphere.historicalcache.Cache;
import com.hazelcast.core.HazelcastInstance;
import com.datasphere.metaRepository.HazelcastSingleton;
import com.datasphere.runtime.NodeStartUp;
import com.datasphere.runtime.RecordKey;

import scala.Option;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Queue;


public abstract class Range implements Serializable, IRange
{
    private static Logger logger;
    private static final long serialVersionUID = 1646284409392129520L;
    private HazelcastInstance hazelcast;
    private static final Range emptyRange;
    
    public Range() {
        this.hazelcast = HazelcastSingleton.get(NodeStartUp.InitializerParams.Params.getWAClusterName());
    }
    
    public abstract IBatch all();
    
    public IBatch lookup(final int indexID, final RecordKey key) {
        return (IBatch)Batch.emptyBatch();
    }
    
    @Override
    public abstract String toString();
    
    public IRange update(final IRange r) {
        return r;
    }
    
    public static Range emptyRange() {
        return Range.emptyRange;
    }
    
    public static Range createRange(final Cache cache) {
        return new Range() {
            private static final long serialVersionUID = -3860895434812640102L;
            private final long TRANSACTION_CLOSED_ID = -1L;
            private Long transactionCount = 0L;
            private long transactionId = -1L;
            
            @Override
            public IBatch all() {
                if (Range.logger.isDebugEnabled()) {
                    Range.logger.debug((Object)("Get all() for cache " + cache.getMetaFullName() + " " + this.transactionId));
                }
                return cache.get(this.transactionId);
            }
            
            @Override
            public IBatch lookup(final int indexID, final RecordKey key) {
                if (Range.logger.isDebugEnabled()) {
                    Range.logger.debug((Object)("Get lookup(K) for cache " + cache.getMetaFullName() + " " + this.transactionId));
                }
                return cache.get(key, this.transactionId);
            }
            
            @Override
            public String toString() {
                return "(cache range ->" + cache + ") with transactionId id " + this.transactionId;
            }
            
            @Override
            public synchronized void beginTransaction() {
                if (this.transactionId == -1L) {
                    this.transactionId = cache.beginTransaction();
                }
                ++this.transactionCount;
            }
            
            @Override
            public synchronized void endTransaction() {
                if (this.transactionId == -1L) {
                    throw new RuntimeException("This transaction is already closed !");
                }
                --this.transactionCount;
                if (this.transactionCount == 0L) {
                    cache.endTransaction(this.transactionId);
                    this.transactionId = -1L;
                }
            }
        };
    }
    
    public static Range createRange(final RecordKey key, final Collection<DARecord> buf) {
        return new SubRange(key) {
            private static final long serialVersionUID = 8217289990303717304L;
            
            public Batch all() {
                return Batch.asBatch(buf);
            }
            
            @Override
            public String toString() {
                return "(collection range for key:" + this.key + ")";
            }
        };
    }
    
    public static Range createRange(final RecordKey key, final Queue<DARecord> buf) {
        return new SubRange(key) {
            private static final long serialVersionUID = 5437575933413950070L;
            
            public Batch all() {
                return Batch.asBatch(buf);
            }
            
            @Override
            public String toString() {
                return "(scala queue range for key:" + this.key + ")";
            }
        };
    }
    
    public static Range createRange(final HashMap<RecordKey, DARecord> map) {
        return new Range() {
            private static final long serialVersionUID = 6264708067898281018L;
            
            public Batch all() {
                return Batch.asBatch(map);
            }
            
            @Override
            public IBatch lookup(final int indexID, final RecordKey key) {
                final Option<DARecord> ret = (Option<DARecord>)map.get(key);
                if (ret.nonEmpty()) {
                    return (IBatch)Batch.asBatch((DARecord)ret.get());
                }
                return (IBatch)Batch.emptyBatch();
            }
            
            @Override
            public String toString() {
                return "(scala map range " + map + ")";
            }
        };
    }
    
    public static IRange createRange(final Map<RecordKey, IBatch> map) {
        return (IRange)new Range() {
            private static final long serialVersionUID = -5114607566419634291L;
            
            public Batch all() {
                return Batch.batchesAsBatch(map.values());
            }
            
            @Override
            public IBatch lookup(final int indexID, final RecordKey key) {
                final IBatch sn = map.get(key);
                if (sn != null) {
                    return sn;
                }
                return (IBatch)Batch.emptyBatch();
            }
            
            @Override
            public IRange update(final IRange r) {
                if (r instanceof SubRange) {
                    final SubRange sr = (SubRange)r;
                    map.put(sr.key, sr.all());
                    return (IRange)this;
                }
                return r;
            }
            
            @Override
            public String toString() {
                return "(map of subranges " + map + ")";
            }
        };
    }
    
    public void beginTransaction() {
    }
    
    public void endTransaction() {
    }
    
    static {
        Range.logger = Logger.getLogger((Class)Range.class);
        emptyRange = new Range() {
            private static final long serialVersionUID = 3638589221501577212L;
            
            public Batch all() {
                return Batch.emptyBatch();
            }
            
            @Override
            public String toString() {
                return "<empty range>";
            }
        };
    }
    
    abstract static class SubRange extends Range
    {
        private static final long serialVersionUID = 1346179739415210631L;
        final RecordKey key;
        
        SubRange(final RecordKey key) {
            this.key = key;
        }
    }
}
