package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.kafkamessaging.StreamPersistencePolicy;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateCqStmt extends CreateStmt
{
    public final String stream_name;
    public final List<String> field_list;
    private List<String> partitionFieldList;
    public final Select select;
    public final String select_text;
    public final String uiConfig;
    private StreamPersistencePolicy persistencePolicy;
    
    public CreateCqStmt(final String cq_name, final Boolean doReplace, final String dest_stream_name, final List<String> field_name_list, final Select sel, final String select_text) {
        super(EntityType.CQ, cq_name, doReplace);
        this.stream_name = dest_stream_name;
        this.field_list = field_name_list;
        this.select = sel;
        this.select_text = select_text;
        this.uiConfig = null;
    }
    
    public CreateCqStmt(final String cq_name, final Boolean doReplace, final String dest_stream_name, final List<String> field_name_list, final Select sel, final String select_text, final String uiConfig) {
        super(EntityType.CQ, cq_name, doReplace);
        this.stream_name = dest_stream_name;
        this.field_list = field_name_list;
        this.select = sel;
        this.select_text = select_text;
        this.uiConfig = uiConfig;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " AS INSERT INTO " + this.stream_name + "(" + this.field_list + ")\n" + this.select;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateCqStmt(this);
    }
    
    public List<String> getPartitionFieldList() {
        return this.partitionFieldList;
    }
    
    public void setPartitionFieldList(final List<String> partitionFieldList) {
        this.partitionFieldList = partitionFieldList;
    }
    
    public StreamPersistencePolicy getPersistencePolicy() {
        return this.persistencePolicy;
    }
    
    public void setPersistencePolicy(final StreamPersistencePolicy persistencePolicy) {
        this.persistencePolicy = persistencePolicy;
    }
}
