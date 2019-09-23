package com.datasphere.runtime.compiler.exprs;

import java.io.*;

public abstract class Predicate extends Expr implements Serializable
{
    public Predicate(final ExprCmd op) {
        super(op);
    }
    
    @Override
    public Class<?> getType() {
        return Boolean.TYPE;
    }
}
