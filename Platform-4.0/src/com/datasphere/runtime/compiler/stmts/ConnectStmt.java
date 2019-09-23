package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class ConnectStmt extends CreateStmt
{
    public final String password;
    public final String clusterid;
    public final String host;
    
    public ConnectStmt(final String username, final String userpass, final String clusterid, final String host) {
        super(EntityType.USER, username, false);
        this.password = userpass;
        this.clusterid = clusterid;
        this.host = host;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileConnectStmt(this);
    }
}
