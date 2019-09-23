package com.datasphere.runtime.window;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import com.datasphere.runtime.KeyFactory;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.containers.Batch;
import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.containers.DARecord;

import scala.Option;
import scala.Tuple2;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.HashMap$;

class PartitionedSlidingCount1BufferPolicy implements HBufferPolicy
{
    final KeyFactory keyFactory;
    HashMap<RecordKey, DARecord> buffer;
    
    PartitionedSlidingCount1BufferPolicy(final KeyFactory kf) {
        this.buffer = HashMap$.MODULE$.empty();
        this.keyFactory = kf;
    }
    
    @Override
    public void updateBuffer(final ScalaWindow w, final Batch batch, final long now) throws Exception {
        final List<DARecord> removed = new ArrayList<DARecord>();
        Iterator iter = batch.iterator();
        while (iter.hasNext()) {
        		final DARecord rec = (DARecord)iter.next();
            final RecordKey key = this.keyFactory.makeKey(rec.data);
            final Option<DARecord> prev = (Option<DARecord>)this.buffer.get(key);
            if (prev.nonEmpty()) {
                removed.add((DARecord)prev.get());
            }
            this.buffer = (HashMap<RecordKey, DARecord>)this.buffer.$plus(new Tuple2(key, (Object)rec));
        }
        w.publish(batch, removed, this.createSnapshot());
    }
    
    @Override
    public void initBuffer(final ScalaWindow w) {
        this.buffer = HashMap$.MODULE$.empty();
    }
    
    @Override
    public void onJumpingTimer() throws Exception {
        throw new UnsupportedOperationException();
    }
    
    @Override
    public Range createSnapshot() {
        return Range.createRange(this.buffer);
    }
}
