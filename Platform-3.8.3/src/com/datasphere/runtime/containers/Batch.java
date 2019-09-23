package com.datasphere.runtime.containers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.StreamEvent;

import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.Queue;

public abstract class Batch<T> implements IBatch
{
    private static final long serialVersionUID = 3418639571053079192L;
    private static final Batch emptyBatch;
    
    public abstract Iterator<T> iterator();
    
    public abstract int size();
    
    public boolean isEmpty() {
        return this.size() == 0;
    }
    
    public T first() {
        final Iterator<T> iterator = super.iterator();
        if (iterator.hasNext()) {
            final T e = iterator.next();
            return e;
        }
        return null;
    }
    
    public T last() {
        T ret = null;
        final Iterator<T> iterator = super.iterator();
        while (iterator.hasNext()) {
            final T e = ret = iterator.next();
        }
        return ret;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        String sep = "";
        sb.append("[");
        for (final Object e : this) {
            sb.append(sep).append(e);
            sep = ",";
        }
        sb.append("]");
        return sb.toString();
    }
    
    public static Batch emptyBatch() {
        return Batch.emptyBatch;
    }
    
    public static Batch asBatch(final HashMap<RecordKey, DARecord> b) {
        return new Batch<DARecord>() {
            private static final long serialVersionUID = 4242550963548968535L;
            
            @Override
            public Iterator<DARecord> iterator() {
                return new Iterator<DARecord>() {
                    scala.collection.Iterator<Tuple2<RecordKey, DARecord>> it = b.iterator();
                    
                    @Override
                    public boolean hasNext() {
                        return this.it.hasNext();
                    }
                    
                    @Override
                    public DARecord next() {
                        return (DARecord)((Tuple2)this.it.next())._2;
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            
            @Override
            public int size() {
                return b.size();
            }
        };
    }
    
    public static Batch asBatch(final Collection<DARecord> col) {
        return new ColBatch(col);
    }
    
    public static Batch asStreamBatch(final Collection<StreamEvent> col) {
        return new StreamColBatch(col);
    }
    
    public static Batch asBatch(final DARecord e) {
        return new Batch<DARecord>() {
            private static final long serialVersionUID = 7274470955311509658L;
            
            @Override
            public Iterator<DARecord> iterator() {
                return new Iterator<DARecord>() {
                    private boolean hasNext = true;
                    
                    @Override
                    public boolean hasNext() {
                        return this.hasNext;
                    }
                    
                    @Override
                    public DARecord next() {
                        if (this.hasNext) {
                            this.hasNext = false;
                            return e;
                        }
                        throw new NoSuchElementException();
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            
            @Override
            public int size() {
                return 1;
            }
        };
    }
    
    public static Batch batchesAsBatch(final Collection<IBatch> col) {
        return new Batch<DARecord>() {
            private static final long serialVersionUID = -1796558864997411339L;
            
            @Override
            public Iterator<DARecord> iterator() {
                return new Iterator<DARecord>() {
                    private Iterator<IBatch> sit = col.iterator();
                    private Iterator<DARecord> it = Collections.emptyIterator();
                    private DARecord _next = null;
                    
                    private DARecord getNext() {
                        if (this._next == null) {
                            while (!this.it.hasNext()) {
                                if (!this.sit.hasNext()) {
                                    return this._next;
                                }
                                this.it = (Iterator<DARecord>)this.sit.next().iterator();
                            }
                            this._next = this.it.next();
                        }
                        return this._next;
                    }
                    
                    @Override
                    public boolean hasNext() {
                        return this.getNext() != null;
                    }
                    
                    @Override
                    public DARecord next() {
                        final DARecord ret = this.getNext();
                        this._next = null;
                        return ret;
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            
            @Override
            public int size() {
                int size = 0;
                for (final IBatch b : col) {
                    size += b.size();
                }
                return size;
            }
            
            @Override
            public boolean isEmpty() {
                if (!col.isEmpty()) {
                    for (final IBatch b : col) {
                        if (!b.isEmpty()) {
                            return false;
                        }
                    }
                }
                return true;
            }
        };
    }
    
    public static Batch asBatch(final Queue<DARecord> buf) {
        return new Batch<DARecord>() {
            private static final long serialVersionUID = 4716758572600013367L;
            
            @Override
            public Iterator<DARecord> iterator() {
                return new Iterator<DARecord>() {
                    private scala.collection.Iterator<DARecord> it = buf.iterator();
                    
                    @Override
                    public boolean hasNext() {
                        return this.it.hasNext();
                    }
                    
                    @Override
                    public DARecord next() {
                        return (DARecord)this.it.next();
                    }
                    
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
            
            @Override
            public int size() {
                return buf.size();
            }
        };
    }
    
    public static Map<RecordKey, IBatch<DARecord>> partition(final KeyFactory keyFactory, final IBatch<DARecord> batch) {
        if (batch.size() == 1) {
            final DARecord rec = (DARecord)batch.first();
            final RecordKey key = keyFactory.makeKey(rec.data);
            return Collections.singletonMap(key, batch);
        }
        final Map<RecordKey, IBatch<DARecord>> map = new LinkedHashMap<RecordKey, IBatch<DARecord>>();
        for (final DARecord rec2 : batch) {
            final RecordKey key2 = keyFactory.makeKey(rec2.data);
            IBatch values = map.get(key2);
            if (values == null) {
                values = (IBatch)new ColBatch(new ArrayList<Object>());
                map.put(key2, (IBatch<DARecord>)values);
            }
            ((ColBatch)values).col.add((DARecord)rec2);
        }
        return map;
    }
    
    static {
        emptyBatch = new Batch<DARecord>() {
            private static final long serialVersionUID = -7983831581625863170L;
            
            @Override
            public Iterator<DARecord> iterator() {
                return Collections.emptyIterator();
            }
            
            @Override
            public int size() {
                return 0;
            }
        };
    }
    
    private static class ColBatch<DARecord> extends Batch
    {
        private static final long serialVersionUID = -6646847362508352538L;
        final Collection<DARecord> col;
        
        ColBatch(final Collection<DARecord> col) {
            this.col = col;
        }
        
        @Override
        public Iterator<DARecord> iterator() {
            return this.col.iterator();
        }
        
        @Override
        public int size() {
            return this.col.size();
        }
    }
    
    private static class StreamColBatch<StreamEvent> extends Batch
    {
        private static final long serialVersionUID = -6646847362508352538L;
        final Collection<StreamEvent> col;
        
        StreamColBatch(final Collection<StreamEvent> col) {
            this.col = col;
        }
        
        @Override
        public Iterator<StreamEvent> iterator() {
            return this.col.iterator();
        }
        
        @Override
        public int size() {
            return this.col.size();
        }
    }
}
