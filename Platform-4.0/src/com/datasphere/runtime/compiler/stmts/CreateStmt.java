package com.datasphere.runtime.compiler.stmts;

import com.datasphere.exception.CompilationException;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public abstract class CreateStmt extends Stmt
{
    public final EntityType what;
    public final String name;
    public final boolean doReplace;
    
    public CreateStmt(final EntityType what, final String name, final boolean doReplace) {
        this.what = what;
        this.name = name;
        this.doReplace = doReplace;
    }
    
    public String stmtToString() {
        return "CREATE " + this.what + (this.doReplace ? " OR REPLACE" : "") + " " + this.name;
    }
    
    @Override
    public String toString() {
        return this.stmtToString();
    }
    
    public void checkValidity(final String exceptionMessage, final Object... args) throws CompilationException {
        if (args == null) {
            return;
        }
        for (final Object arg : args) {
            if (arg == null) {
                throw new CompilationException(exceptionMessage);
            }
        }
    }
    
    public boolean isWithDebugEnabled() {
        final TraceOptions traceOptions = Compiler.buildTraceOptions(this);
        return traceOptions.traceFlags == 64;
    }
}
