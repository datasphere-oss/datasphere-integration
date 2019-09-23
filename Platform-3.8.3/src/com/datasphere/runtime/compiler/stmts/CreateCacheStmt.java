package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.components.EntityType;

public class CreateCacheStmt extends CreateStmt
{
    public final AdapterDescription src;
    public final AdapterDescription parser;
    public final List<Property> query_props;
    public final String typename;
    
    public CreateCacheStmt(final EntityType what, final String name, final Boolean doReplace, final AdapterDescription src, final AdapterDescription parser, final List<Property> query_props, final String typename) {
        super(what, name, doReplace);
        this.src = src;
        this.parser = parser;
        this.query_props = query_props;
        this.typename = typename;
    }
    
    @Override
    public String toString() {
        String stmtString = null;
        stmtString = this.stmtToString();
        stmtString = stmtString + " " + this.src.getAdapterTypeName() + " (" + this.src.getProps() + ") ";
        if (this.parser.getAdapterTypeName() != null) {
            stmtString = stmtString + "PARSE USING " + this.parser.getAdapterTypeName() + " (" + this.src.getProps() + ") ";
        }
        stmtString = stmtString + "QUERY " + this.query_props + " TYPE " + this.typename;
        return stmtString;
    }
    
    @Override
    public Object compile(final Compiler c) throws MetaDataRepositoryException {
        return c.compileCreateCacheStmt(this);
    }
}
