package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateDeploymentGroupStmt extends CreateStmt
{
    public final List<String> deploymentGroup;
    public final Long minServers;
    public final Long maxApps;
    
    public CreateDeploymentGroupStmt(final String groupname, final List<String> deploymentGroup, final Long minServers, final Long maxApps) {
        super(EntityType.DG, groupname, false);
        this.deploymentGroup = deploymentGroup;
        this.minServers = minServers;
        this.maxApps = maxApps;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " (" + this.deploymentGroup + ")";
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateDeploymentGroupStmt(this);
    }
}
