package com.datasphere.runtime.compiler.exprs;

import java.lang.reflect.*;
import java.util.*;
import com.datasphere.runtime.compiler.visitors.*;
import org.apache.commons.lang.builder.*;
import com.datasphere.runtime.compiler.custom.*;
import java.io.*;

public class FuncCall extends Operation implements Serializable
{
    protected static Method dummyMethod;
    private String name;
    private int kind;
    protected Method method;
    private int index;
    
    protected static void m() {
    }
    
    public FuncCall(final String funcName, final List<ValueExpr> args, final int options) {
        super(ExprCmd.FUNC, args);
        this.index = -1;
        this.name = funcName;
        this.kind = options;
        this.method = FuncCall.dummyMethod;
    }
    
    @Override
    public String toString() {
        return this.exprToString() + this.name + "(" + this.argsToString() + ")";
    }
    
    @Override
    public <T> Expr visit(final ExpressionVisitor<T> visitor, final T params) {
        return visitor.visitFuncCall(this, params);
    }
    
    public void setMethodRef(final Method meth) {
        this.method = meth;
    }
    
    public CustomFunctionTranslator isCustomFunc() {
        final CustomFunction desc = this.method.getAnnotation(CustomFunction.class);
        if (desc == null) {
            return null;
        }
        try {
            final Object o = desc.translator().newInstance();
            return (CustomFunctionTranslator)o;
        }
        catch (InstantiationException | IllegalAccessException ex2) {
            return null;
        }
    }
    
    public AggHandlerDesc getAggrDesc() {
        return this.method.getAnnotation(AggHandlerDesc.class);
    }
    
    public boolean acceptWildcard() {
        return this.method.getAnnotation(AcceptWildcard.class) != null;
    }
    
    public boolean isDistinct() {
        return (this.kind & 0x4) != 0x0;
    }
    
    public Class<?> getAggrClass() {
        final AggHandlerDesc desc = this.getAggrDesc();
        if (desc == null) {
            return null;
        }
        if (!this.isDistinct()) {
            return desc.handler();
        }
        final Class<?> c = desc.distinctHandler();
        return (c == Object.class) ? null : c;
    }
    
    public Method getMethd() {
        return this.method;
    }
    
    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof FuncCall)) {
            return false;
        }
        final FuncCall o = (FuncCall)other;
        return super.equals(o) && this.kind == o.kind && this.method.equals(o.method);
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.kind).append((Object)this.method).append(super.hashCode()).toHashCode();
    }
    
    public ValueExpr getThis() {
        return null;
    }
    
    public List<ValueExpr> getFuncArgs() {
        return this.args;
    }
    
    @Override
    public boolean isConst() {
        if (this.getArgs().isEmpty()) {
            return false;
        }
        assert this.method != null;
        return this.method.getAnnotation(Nondeterministic.class) == null && this.method.getAnnotation(CustomFunction.class) == null && this.getAggrDesc() == null && super.isConst();
    }
    
    @Override
    public Class<?> getType() {
        return this.method.getReturnType();
    }
    
    public int getIndex() {
        return this.index;
    }
    
    public void setIndex(final int index) {
        this.index = index;
    }
    
    public String getName() {
        return this.name;
    }
    
    public boolean isValidated() {
        return this.method != FuncCall.dummyMethod;
    }
    
    private void writeObject(final ObjectOutputStream out) throws IOException {
        out.writeUTF(this.name);
        out.writeInt(this.kind);
        out.writeInt(this.index);
        out.writeObject(this.method.getDeclaringClass());
        out.writeUTF(this.method.getName());
        out.writeObject(this.method.getParameterTypes());
    }
    
    private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.name = in.readUTF();
        this.kind = in.readInt();
        this.index = in.readInt();
        final Class<?> declaringClass = (Class<?>)in.readObject();
        final String methodName = in.readUTF();
        final Class<?>[] parameterTypes = (Class<?>[])in.readObject();
        try {
            this.method = declaringClass.getMethod(methodName, parameterTypes);
        }
        catch (Exception e) {
            this.method = FuncCall.dummyMethod;
        }
    }
    
    static {
        try {
            FuncCall.dummyMethod = FuncCall.class.getDeclaredMethod("m", (Class<?>[])new Class[0]);
        }
        catch (NoSuchMethodException | SecurityException ex2) {
            assert false;
        }
    }
}
