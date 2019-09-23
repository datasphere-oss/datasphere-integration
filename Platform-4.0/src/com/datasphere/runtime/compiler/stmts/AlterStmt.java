package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class AlterStmt extends Stmt
{
    private final String objectName;
    private final Boolean enablePartitionBy;
    private final StreamPersistencePolicy persistencePolicy;
    private final Boolean enablePersistence;
    private final List<String> partitionBy;
    
    public AlterStmt(final String objectName, final Boolean enablePartitionBy, final List<String> partitionBy, final Boolean enablePersistence, final StreamPersistencePolicy persistencePolicy) {
        this.objectName = objectName;
        this.enablePartitionBy = enablePartitionBy;
        this.partitionBy = partitionBy;
        this.enablePersistence = enablePersistence;
        this.persistencePolicy = persistencePolicy;
    }
    
    public String getObjectName() {
        return this.objectName;
    }
    
    public StreamPersistencePolicy getPersistencePolicy() {
        return this.persistencePolicy;
    }
    
    public Boolean getEnablePersistence() {
        return this.enablePersistence;
    }
    
    public List<String> getPartitionBy() {
        return this.partitionBy;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileAlterStmt(this);
    }
    
    public Boolean getEnablePartitionBy() {
        return this.enablePartitionBy;
    }
    
    @Override
    public String toString() {
        return this.objectName + " " + this.enablePartitionBy + " " + this.persistencePolicy + " " + this.enablePersistence + "  " + this.partitionBy;
    }
}
