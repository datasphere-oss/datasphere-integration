package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateVisualizationStmt extends CreateStmt
{
    public String fileName;
    
    public CreateVisualizationStmt(final String objectName, final String filename) {
        super(EntityType.VISUALIZATION, objectName, false);
        this.fileName = filename;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateVisualizationStmt(this);
    }
}
