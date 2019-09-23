package com.datasphere.runtime.compiler.exprs;

import java.io.*;
import com.datasphere.runtime.compiler.visitors.*;
import java.util.*;

public abstract class Expr implements Serializable
{
    private static int nextExprID;
    public final ExprCmd op;
    public final int eid;
    public Expr originalExpr;
    
    public Expr(final ExprCmd op) {
        this.op = op;
        this.eid = ++Expr.nextExprID;
    }
    
    public String exprToString() {
        return this.op + "#" + this.eid + " ";
    }
    
    @Override
    public String toString() {
        return this.exprToString();
    }
    
    public abstract Class<?> getType();
    
    public abstract List<? extends Expr> getArgs();
    
    public abstract <T> Expr visit(final ExpressionVisitor<T> p0, final T p1);
    
    public abstract <T> void visitArgs(final ExpressionVisitor<T> p0, final T p1);
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof Expr)) {
            return false;
        }
        final Expr o = (Expr)other;
        return this.op.equals(o.op) && this.getType().equals(o.getType()) && this.getArgs().equals(o.getArgs());
    }
    
    @Override
    public int hashCode() {
        return this.op.hashCode();
    }
    
    public static Class<?>[] getSignature(final Collection<? extends Expr> exprs) {
        final Class<?>[] sig = (Class<?>[])new Class[exprs.size()];
        int i = 0;
        for (final Expr e : exprs) {
            sig[i++] = e.getType();
        }
        return sig;
    }
    
    public boolean isTypeRef() {
        return false;
    }
    
    public Expr resolve() {
        return this;
    }
    
    public boolean isConst() {
        for (final Expr e : this.getArgs()) {
            if (!e.isConst()) {
                return false;
            }
        }
        return true;
    }
    
    public Expr setOriginalExpr(final Expr e) {
        if (e != this) {
            this.originalExpr = e;
        }
        return this;
    }
    
    public final String getTypeName() {
        return this.getType().getCanonicalName();
    }
    
    static {
        Expr.nextExprID = 0;
    }
}
