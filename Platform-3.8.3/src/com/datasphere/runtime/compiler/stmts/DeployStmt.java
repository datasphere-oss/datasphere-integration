package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class DeployStmt extends Stmt
{
    public final EntityType appOrFlow;
    public final DeploymentRule appRule;
    public final List<DeploymentRule> flowRules;
    public final List<Pair<String, String>> options;
    
    public DeployStmt(final EntityType type, final DeploymentRule appRule, final List<DeploymentRule> flowRules, final List<Pair<String, String>> options) {
        this.appOrFlow = type;
        this.appRule = appRule;
        this.flowRules = flowRules;
        this.options = options;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileDeployStmt(this);
    }
    
    @Override
    public String toString() {
        if (this.sourceText != null && !this.sourceText.isEmpty()) {
            return this.sourceText;
        }
        final StringBuilder sb = new StringBuilder();
        sb.append("DEPLOY APPLICATION ");
        if (this.appRule != null) {
            sb.append(this.appRule.toString());
        }
        if (this.flowRules != null && this.flowRules.size() > 0) {
            sb.append(" WITH ");
            String temp = "";
            for (final DeploymentRule dr : this.flowRules) {
                temp = temp + dr.toString() + ",";
            }
            temp = temp.substring(0, temp.length() - 1);
            sb.append(temp);
        }
        return sb.toString();
    }
}
