package com.datasphere.runtime.compiler.stmts;

import org.apache.log4j.Logger;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.tungsten.Tungsten;

public class PrintMetaDataStmt extends Stmt
{
    String mode;
    String type;
    String command;
    private static Logger logger;
    
    public PrintMetaDataStmt(final String mode, final String type, final String command) {
        this.mode = "";
        this.type = null;
        this.command = "";
        this.mode = mode.toLowerCase();
        this.type = type;
        this.command = command;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        final String mode = this.mode;
        switch (mode) {
            case "list": {
                Tungsten.list(this.mode + " " + this.type + "");
                break;
            }
            case "describe": {
                Tungsten.describe(this.mode + " " + ((this.type == null) ? "" : (this.type + " ")) + this.command);
                break;
            }
        }
        return null;
    }
    
    static {
        PrintMetaDataStmt.logger = Logger.getLogger((Class)PrintMetaDataStmt.class);
    }
}
