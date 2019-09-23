package com.datasphere.runtime.window;

import java.util.Map;

import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.containers.DARecord;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.HashMap$;

class PartitionedBufferPolicy implements HBufferPolicy
{
    final KeyFactory keyFactory;
    HashMap<RecordKey, HBuffer> buffers;
    
    PartitionedBufferPolicy(final KeyFactory kf) throws Exception {
        this.buffers = HashMap$.MODULE$.empty();
        this.keyFactory = kf;
    }
    
    private HBuffer getWindowByKey(final ScalaWindow w, final RecordKey key) {
        final Option<HBuffer> entry = (Option<HBuffer>)this.buffers.get(key);
        HBuffer b;
        if (entry.isEmpty()) {
            b = w.createBuffer(key);
            this.buffers = (HashMap<RecordKey, HBuffer>)this.buffers.$plus(new Tuple2(key, b));
        }
        else {
            b = (HBuffer)entry.get();
        }
        return b;
    }
    
    @Override
    public void updateBuffer(final ScalaWindow w, final Batch batch, final long now) throws Exception {
        final Map<RecordKey, IBatch<DARecord>> map = Batch.partition(this.keyFactory, batch);
        for (final Map.Entry<RecordKey, IBatch<DARecord>> e : map.entrySet()) {
            final RecordKey key = e.getKey();
            final IBatch keybatch = e.getValue();
            final HBuffer b = this.getWindowByKey(w, key);
            w.updateBuffer(b, (Batch)keybatch, now);
        }
    }
    
    @Override
    public void initBuffer(final ScalaWindow w) {
        this.buffers = HashMap$.MODULE$.empty();
    }
    
    @Override
    public void onJumpingTimer() throws Exception {
    		Iterator iter = this.buffers.iterator();
        while (iter.hasNext()) {
        	final Tuple2<RecordKey, HBuffer> t = (Tuple2<RecordKey, HBuffer>)iter.next();
            try {
                ((HBuffer)t._2).removeAll();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    @Override
    public Range createSnapshot() {
        final Map<RecordKey, IBatch> map = new java.util.HashMap<RecordKey, IBatch>();
        Iterator iter = this.buffers.iterator();
        while (iter.hasNext()) {
        		final Tuple2<RecordKey, HBuffer> t = (Tuple2<RecordKey, HBuffer>)iter.next();
            map.put((RecordKey)t._1, ((HBuffer)t._2).makeSnapshot().all());
        }
        final Range range = (Range)Range.createRange(map);
        return range;
    }
}
