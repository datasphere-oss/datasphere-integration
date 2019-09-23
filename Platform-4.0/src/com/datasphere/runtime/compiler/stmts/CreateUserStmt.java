package com.datasphere.runtime.compiler.stmts;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.runtime.meta.MetaInfo;

public class CreateUserStmt extends CreateStmt
{
    public final String password;
    public final UserProperty prop;
    
    public CreateUserStmt(final String userid, final String password, final UserProperty prop, final String ldap, final String aliasName) {
        super(EntityType.USER, userid, false);
        this.password = password;
        this.prop = prop;
        if (ldap != null) {
            prop.originType = MetaInfo.User.AUTHORIZATION_TYPE.LDAP;
            prop.ldap = ldap;
        }
        prop.setAlias(aliasName);
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateUserStmt(this);
    }
}
