package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreatePropertySetStmt extends CreateStmt
{
    public final List<Property> props;
    
    public CreatePropertySetStmt(final String name, final Boolean doReplace, final List<Property> props) {
        super(EntityType.PROPERTYSET, name, doReplace);
        this.props = props;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " (" + this.props + ")";
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreatePropertySetStmt(this);
    }
}
