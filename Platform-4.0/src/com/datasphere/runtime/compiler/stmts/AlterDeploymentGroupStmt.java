package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;

public class AlterDeploymentGroupStmt extends Stmt
{
    public final String groupname;
    public final List<String> nodesRemoved;
    public final List<String> nodesAdded;
    public final Long minServers;
    public final Long limiApplications;
    
    public AlterDeploymentGroupStmt(final String groupname, final List<String> nodesAdded, final List<String> nodesRemoved, final Long minServers, final Long limitApplications) {
        this.groupname = groupname;
        this.nodesAdded = nodesAdded;
        this.nodesRemoved = nodesRemoved;
        this.minServers = minServers;
        this.limiApplications = limitApplications;
    }
    
    @Override
    public String toString() {
        final StringBuilder returnString = new StringBuilder();
        if (this.nodesAdded != null) {
            returnString.append(" (nodes added:" + this.nodesAdded + ")");
        }
        if (this.nodesRemoved != null) {
            returnString.append(" (nodes removed:" + this.nodesRemoved + ")");
        }
        if (this.minServers != null) {
            returnString.append(" (minimum servers:" + this.minServers + ")");
        }
        if (this.limiApplications != null) {
            returnString.append(" (limit applications:" + this.limiApplications + ")");
        }
        return returnString.toString();
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileAlterDeploymentGroupStmt(this);
    }
}
