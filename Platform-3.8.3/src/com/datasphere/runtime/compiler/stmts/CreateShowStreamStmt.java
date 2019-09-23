package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.tungsten.Tungsten;

public class CreateShowStreamStmt extends Stmt
{
    public final String stream_name;
    public final int line_count;
    public final boolean isTungsten;
    
    public CreateShowStreamStmt(final String stream_name) {
        this.stream_name = stream_name;
        this.line_count = 10;
        Tungsten.isAdhocRunning.set(true);
        this.isTungsten = true;
    }
    
    public CreateShowStreamStmt(final String stream_name, final int line_count) {
        this.stream_name = stream_name;
        this.line_count = line_count;
        Tungsten.isAdhocRunning.set(true);
        this.isTungsten = true;
    }
    
    public CreateShowStreamStmt(final String stream_name, final int line_count, final boolean isTungsten) {
        this.stream_name = stream_name;
        this.line_count = line_count;
        this.isTungsten = isTungsten;
    }
    
    @Override
    public String toString() {
        return "SHOW " + this.stream_name + " line_count " + this.line_count;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileShowStreamStmt(this);
    }
}
