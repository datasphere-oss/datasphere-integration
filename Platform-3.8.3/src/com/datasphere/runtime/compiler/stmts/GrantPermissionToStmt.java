package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.security.ObjectPermission;

public class GrantPermissionToStmt extends Stmt implements SecurityStmt
{
    public final EntityType towhat;
    List<ObjectPermission.Action> listOfPrivilege;
    List<ObjectPermission.ObjectType> objectType;
    public String objectName;
    public String name;
    
    public GrantPermissionToStmt(@NotNull final List<ObjectPermission.Action> listOfPrivilege, @Nullable final List<ObjectPermission.ObjectType> objectType, @NotNull final String name, @Nullable final String userorpermissionname, final EntityType what) {
        this.towhat = what;
        this.listOfPrivilege = listOfPrivilege;
        this.objectType = objectType;
        this.objectName = name;
        this.name = userorpermissionname;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.GrantPermissionToStmt(this);
    }
    
    @Override
    public String getName() {
        return this.objectName;
    }
    
    @Override
    public List<ObjectPermission.Action> getListOfPrivilege() {
        return this.listOfPrivilege;
    }
    
    @Override
    public List<ObjectPermission.ObjectType> getObjectType() {
        return this.objectType;
    }
}
