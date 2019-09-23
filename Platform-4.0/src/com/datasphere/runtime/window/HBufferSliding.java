package com.datasphere.runtime.window;

import scala.collection.immutable.*;
import scala.collection.immutable.Queue;

import com.datasphere.runtime.containers.*;
import com.datasphere.runtime.containers.Range;

import java.util.*;
import scala.*;

abstract class HBufferSliding extends HBuffer
{
    private Queue<DARecord> buffer;
    
    HBufferSliding() {
        this.buffer = Queue$.MODULE$.empty();
    }
    
    @Override
    final void addEvent(final DARecord e) {
        this.buffer = this.buffer.enqueue(e);
    }
    
    @Override
    final DARecord getFirstEvent() throws NoSuchElementException {
        return (DARecord)this.buffer.front();
    }
    
    @Override
    final int getSize() {
        return this.buffer.size();
    }
    
    @Override
    Range makeSnapshot() {
        return Range.createRange(this.key, this.buffer);
    }
    
    final void removeEvent(final Collection<DARecord> removed) {
        final Tuple2<DARecord, Queue<DARecord>> ret = (Tuple2<DARecord, Queue<DARecord>>)this.buffer.dequeue();
        this.buffer = (Queue<DARecord>)ret._2;
        removed.add((DARecord)ret._1);
    }
}
