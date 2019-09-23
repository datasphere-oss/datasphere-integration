package com.datasphere.runtime.compiler.select;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.compiler.exprs.CastExprFactory;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.compiler.exprs.ParamRef;
import com.datasphere.runtime.compiler.exprs.StaticFieldRef;
import com.datasphere.runtime.compiler.exprs.TypeRef;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.utils.NameHelper;
import com.datasphere.runtime.utils.NamePolicy;

import javassist.Modifier;

public abstract class ExprValidator
{
    private final Compiler compiler;
    private final Map<String, ParamRef> parameters;
    private final boolean acceptAgg;
    private final boolean acceptWildcard;
    
    public ExprValidator(final Compiler compiler, final Map<String, ParamRef> parameters, final boolean acceptAgg, final boolean acceptWildcard) {
        this.compiler = compiler;
        this.parameters = parameters;
        this.acceptAgg = acceptAgg;
        this.acceptWildcard = acceptWildcard;
    }
    
    public final void error(final String msg, final Object info) {
        this.compiler.error(msg, info);
    }
    
    public final Class<?> getTypeInfo(final TypeName t) {
        return this.compiler.getClass(t);
    }
    
    public final ValueExpr cast(final ValueExpr expr, final Class<?> targetType) {
        return CastExprFactory.cast(this.compiler, expr, targetType);
    }
    
    public final List<Class<?>> getListOfStaticMethods(final String funcName) {
        final Context ctx = this.compiler.getContext();
        final List<Class<?>> results = ctx.getImports().getStaticMethodRef(funcName, BuiltInFunc.class);
        if (results != null && results.size() > 1) {
            for (int i = 0; i < results.size(); ++i) {
                if (!results.get(i).equals(BuiltInFunc.class) && ctx.getCurApp() != null) {
                    ctx.getCurApp().importStatements.add("IMPORT STATIC " + results.get(i).getCanonicalName() + ".*;");
                }
            }
        }
        return results;
    }
    
    public final ValueExpr findStaticVariableOrClass(final String name) {
        final Context ctx = this.compiler.getContext();
        if (name.indexOf(46) != -1) {
            try {
                final Class<?> c = ctx.getClass(NameHelper.getPrefix(name));
                final Field f = NamePolicy.getField(c, NameHelper.getBasename(name));
                if (Modifier.isStatic(f.getModifiers())) {
                    return new StaticFieldRef(f);
                }
                return null;
            }
            catch (ClassNotFoundException | NoSuchFieldException | SecurityException ex2) {
                try {
                    final Class<?> c2 = ctx.getClass(name.toString());
                    return new TypeRef(c2);
                }
                catch (ClassNotFoundException ex3) {}
            }
        }
        final Field f = ctx.getImports().getStaticFieldRef(name);
        if (f != null) {
            return new StaticFieldRef(f);
        }
        try {
            final Class<?> c3 = ctx.getClass(name);
            return new TypeRef(c3);
        }
        catch (ClassNotFoundException ex4) {}
        return null;
    }
    
    public Expr addParameter(final ParamRef pref) {
        ParamRef ref = this.parameters.get(pref.getName());
        if (ref == null) {
            final int index = this.parameters.size();
            pref.setIndex(index);
            this.parameters.put(pref.getName(), pref);
            ref = pref;
        }
        return ref;
    }
    
    public boolean acceptAgg() {
        return this.acceptAgg;
    }
    
    public boolean acceptWildcard() {
        return this.acceptWildcard;
    }
    
    public void setHaveAggFuncs() {
    }
    
    public DataSet getDefaultDataSet() {
        return null;
    }
    
    public abstract Expr resolveAlias(final String p0);
    
    public abstract DataSet getDataSet(final String p0);
    
    public abstract FieldRef findField(final String p0);
}
