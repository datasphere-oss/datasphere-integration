package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class LoadUnloadJarStmt extends Stmt
{
    public final String pathToJar;
    public final boolean doLoad;
    
    public LoadUnloadJarStmt(final String pathToJar, final boolean doLoad) {
        this.pathToJar = pathToJar;
        this.doLoad = doLoad;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileLoadOrUnloadJar(this);
    }
}
