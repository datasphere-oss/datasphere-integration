package com.datasphere.runtime.window;

import java.util.List;
import java.util.NoSuchElementException;

import com.datasphere.runtime.containers.Range;
import com.datasphere.runtime.containers.DARecord;

import scala.collection.JavaConverters$;
import scala.collection.immutable.Queue;
import scala.collection.immutable.Queue$;
import scala.collection.immutable.Seq;

abstract class HBufferJumping1 extends HBuffer
{
    private Queue<DARecord> buffer;
    private Queue<DARecord> oldbuf;
    
    HBufferJumping1() {
        this.buffer = Queue$.MODULE$.empty();
        this.oldbuf = Queue$.MODULE$.empty();
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
        return Range.createRange(this.key, this.oldbuf);
    }
    
    void doJump() throws Exception {
        final List<DARecord> newrec = (List<DARecord>)JavaConverters$.MODULE$.seqAsJavaListConverter((Seq)this.buffer).asJava();
        final List<DARecord> oldrec = (List<DARecord>)JavaConverters$.MODULE$.seqAsJavaListConverter((Seq)this.oldbuf).asJava();
        this.oldbuf = this.buffer;
        this.buffer = Queue$.MODULE$.empty();
        this.publish(newrec, oldrec);
    }
}
