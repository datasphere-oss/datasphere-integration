package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.IntervalPolicy;

public class CreateWindowStmt extends CreateStmt
{
    public final String stream_name;
    public final Pair<IntervalPolicy, IntervalPolicy> window_len;
    public final boolean jumping;
    public final List<String> partitioning_fields;
    public final Pair<IntervalPolicy, IntervalPolicy> slidePolicy;
    
    public CreateWindowStmt(final String window_name, final Boolean doReplace, final String stream_name, final Pair<IntervalPolicy, IntervalPolicy> window_len, final boolean isJumping, final List<String> part, final Pair<IntervalPolicy, IntervalPolicy> slidePolicy) {
        super(EntityType.WINDOW, window_name, doReplace);
        this.stream_name = stream_name;
        this.window_len = window_len;
        this.jumping = isJumping;
        this.partitioning_fields = part;
        this.slidePolicy = slidePolicy;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " OVER " + this.stream_name + " KEEP " + this.window_len + (this.jumping ? " JUMPING " : "") + ((this.partitioning_fields == null) ? "" : ("PARTITION " + this.partitioning_fields));
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateWindowStmt(this);
    }
}
