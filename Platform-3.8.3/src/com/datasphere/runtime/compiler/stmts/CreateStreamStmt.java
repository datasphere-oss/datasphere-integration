package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeField;
import com.datasphere.runtime.components.EntityType;

public class CreateStreamStmt extends CreateStmt
{
    public final String typeName;
    public final List<TypeField> fields;
    public final List<String> partitioning_fields;
    public final GracePeriod gracePeriod;
    public final StreamPersistencePolicy spp;
    
    public CreateStreamStmt(final String streamName, final Boolean doReplace, final List<String> partition_fields, final String typeName, final List<TypeField> fields, final GracePeriod gp, final StreamPersistencePolicy spp) {
        super(EntityType.STREAM, streamName, doReplace);
        this.partitioning_fields = partition_fields;
        this.typeName = typeName;
        this.fields = fields;
        this.gracePeriod = gp;
        this.spp = spp;
    }
    
    @Override
    public String toString() {
        final String t = (this.typeName != null) ? (" OF " + this.typeName) : (" (" + this.fields + ")");
        return this.stmtToString() + t + ((this.gracePeriod == null) ? "" : (" " + this.gracePeriod));
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateStreamStmt(this);
    }
}
