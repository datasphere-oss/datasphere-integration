package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.Compiler;

public class UpdateUserInfoStmt extends Stmt
{
    public final String username;
    public List<Property> properties;
    
    public UpdateUserInfoStmt(final String username, final List<Property> props) {
        this.username = username;
        this.properties = props;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.UpdateUserInfoStmt(this);
    }
    
    @Override
    public String getRedactedSourceText() {
        for (final Property p : this.properties) {
            if (StringUtils.containsIgnoreCase((CharSequence)p.name, (CharSequence)"password")) {
                return "alter user " + this.username + " set (REDACTED PASSWORDS)";
            }
        }
        return this.sourceText;
    }
}
