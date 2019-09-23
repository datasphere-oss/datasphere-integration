package com.datasphere.runtime.compiler.stmts;

import java.util.List;
import java.util.Set;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateAppOrFlowStatement extends CreateStmt
{
    public final List<Pair<EntityType, String>> entities;
    public final RecoveryDescription recoveryDesc;
    public final ExceptionHandler eh;
    public boolean encrypted;
    public final Set<String> importStatements;
    public final List<DeploymentRule> deploymentRules;
    
    public CreateAppOrFlowStatement(final EntityType type, final String name, final Boolean doReplace, final List<Pair<EntityType, String>> entities, final Boolean encrypt, final RecoveryDescription recov, final ExceptionHandler eh, final Set<String> importStatements, final List<DeploymentRule> deploymentRules) {
        super(type, name, doReplace);
        this.encrypted = false;
        this.entities = entities;
        this.recoveryDesc = recov;
        if (type.ordinal() == EntityType.APPLICATION.ordinal()) {
            this.encrypted = encrypt;
        }
        this.eh = eh;
        this.importStatements = importStatements;
        this.deploymentRules = deploymentRules;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " (" + this.entities + ")";
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateAppOrFlowStatement(this);
    }
}
