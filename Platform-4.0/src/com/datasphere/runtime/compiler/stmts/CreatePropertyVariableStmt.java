package com.datasphere.runtime.compiler.stmts;

import java.util.ArrayList;
import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreatePropertyVariableStmt extends CreateStmt
{
    public final List<Property> props;
    
    public CreatePropertyVariableStmt(final String paramname, final Object paramvalue, final Boolean doReplace) {
        super(EntityType.PROPERTYVARIABLE, paramname, doReplace);
        final List<Property> props = new ArrayList<Property>();
        final Property prop1 = new Property(paramname, paramvalue);
        props.add(prop1);
        final Object obj = false;
        final Property prop2 = new Property(paramname + "_encrypted", obj);
        props.add(prop2);
        this.props = props;
    }
    
    @Override
    public String toString() {
        return this.stmtToString() + " (" + this.props + ")";
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreatePropertyVariableStmt(this);
    }
}
