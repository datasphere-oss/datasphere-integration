package com.datasphere.runtime.compiler.visitors;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;

import com.fasterxml.jackson.databind.JsonNode;
import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.compiler.custom.CustomFunctionTranslator;
import com.datasphere.runtime.compiler.exprs.ArrayInitializer;
import com.datasphere.runtime.compiler.exprs.ArrayTypeRef;
import com.datasphere.runtime.compiler.exprs.Case;
import com.datasphere.runtime.compiler.exprs.CaseExpr;
import com.datasphere.runtime.compiler.exprs.CastExpr;
import com.datasphere.runtime.compiler.exprs.CastOperation;
import com.datasphere.runtime.compiler.exprs.ClassRef;
import com.datasphere.runtime.compiler.exprs.ComparePredicate;
import com.datasphere.runtime.compiler.exprs.Constant;
import com.datasphere.runtime.compiler.exprs.ConstructorCall;
import com.datasphere.runtime.compiler.exprs.DataSetRef;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.compiler.exprs.FuncCall;
import com.datasphere.runtime.compiler.exprs.IndexExpr;
import com.datasphere.runtime.compiler.exprs.InstanceOfPredicate;
import com.datasphere.runtime.compiler.exprs.IntegerOperation;
import com.datasphere.runtime.compiler.exprs.JsonFieldRef;
import com.datasphere.runtime.compiler.exprs.LogicalPredicate;
import com.datasphere.runtime.compiler.exprs.NumericOperation;
import com.datasphere.runtime.compiler.exprs.ObjectRef;
import com.datasphere.runtime.compiler.exprs.Operation;
import com.datasphere.runtime.compiler.exprs.ParamRef;
import com.datasphere.runtime.compiler.exprs.PatternVarRef;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.RowSetRef;
import com.datasphere.runtime.compiler.exprs.StaticFieldRef;
import com.datasphere.runtime.compiler.exprs.StringConcatenation;
import com.datasphere.runtime.compiler.exprs.TypeRef;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.exprs.WildCardExpr;
import com.datasphere.runtime.compiler.select.DataSet;
import com.datasphere.runtime.compiler.select.ExprValidator;
import com.datasphere.runtime.compiler.visitors.ExprValidationVisitor.AFVparams;
import com.datasphere.runtime.exceptions.AmbiguousSignature;
import com.datasphere.runtime.exceptions.SignatureNotFound;

public class ExprValidationVisitor implements ExpressionVisitor<AFVparams>
{
    private final ExprValidator ctx;
    private Expr parent;
    private static final boolean rewriteBuiltInOp = false;
    
    public ExprValidationVisitor(final ExprValidator ctx) {
        this.ctx = ctx;
    }
    
    public Expr validate(final Expr e) {
        this.parent = null;
        final Expr result = this.visitExpr(e, new AFVparams());
        return result.resolve();
    }
    
    private void error(final String msg, final Object info) {
        this.ctx.error(msg, info);
    }
    
    private Class<?> getTypeInfo(final TypeName t) {
        return this.ctx.getTypeInfo(t);
    }
    
    private ValueExpr cast(final ValueExpr expr, final Class<?> targetType) {
        return this.ctx.cast(expr, targetType);
    }
    
    @Override
    public Expr visitExpr(Expr e, final AFVparams params) {
        final AFVparams p = new AFVparams();
        final Expr save_parent = this.parent;
        (this.parent = e).visitArgs(this, p);
        this.parent = save_parent;
        final Expr newe = e.visit(this, p);
        if (newe != null) {
            e = newe;
        }
        params.seenList.add(p.wasSeen() || p.wasSeenHere);
        return e;
    }
    
    @Override
    public Expr visitExprDefault(final Expr e, final AFVparams params) {
        assert false;
        return null;
    }
    
    private static boolean hasAnyStringArg(final List<ValueExpr> args) {
        for (final ValueExpr arg : args) {
            if (arg.getType().equals(String.class)) {
                return true;
            }
        }
        return false;
    }
    
    private static WildCardExpr findWildCardExpr(final List<ValueExpr> args) {
        for (final ValueExpr arg : args) {
            if (arg instanceof WildCardExpr) {
                return (WildCardExpr)arg;
            }
        }
        return null;
    }
    
    private void castArgsTo(final List<ValueExpr> args, final Class<?> resulttype) {
        final ListIterator<ValueExpr> it = args.listIterator();
        while (it.hasNext()) {
            final ValueExpr e = it.next();
            final ValueExpr enew = this.cast(e, resulttype);
            it.set(enew);
        }
    }
    
    @Override
    public Expr visitNumericOperation(final NumericOperation operation, final AFVparams params) {
        final boolean isInteger = operation instanceof IntegerOperation;
        if (operation.op == ExprCmd.PLUS && hasAnyStringArg(operation.args)) {
            final WildCardExpr wc = findWildCardExpr(operation.args);
            if (wc != null) {
                this.error("syntax error", wc);
            }
            final Operation newop = new StringConcatenation(operation.args);
            return newop.setOriginalExpr(operation);
        }
        final Class<?> widestType = (Class<?>)(isInteger ? Long.TYPE : Double.TYPE);
        Class<?> resulttype = Integer.TYPE;
        for (final ValueExpr arg : operation.args) {
            final Class<?> argType = arg.getType();
            if (!CompilerUtils.isConvertibleWithBoxing(widestType, argType)) {
                this.error("invalid type of operand", arg);
            }
            resulttype = CompilerUtils.getCommonSuperType(resulttype, arg.getType());
            if (resulttype == null) {
                this.error("incompatible expression type", arg);
            }
        }
        resulttype = CompilerUtils.getDefaultTypeIfCannotInfer(resulttype, Integer.TYPE);
        operation.setType(resulttype);
        this.castArgsTo(operation.args, resulttype);
        return null;
    }
    
    @Override
    public Expr visitConstant(final Constant constant, final AFVparams params) {
        return null;
    }
    
    @Override
    public Expr visitCase(final CaseExpr caseExpr, final AFVparams params) {
        int i = 0;
        Class<?> resulttype = null;
        Class<?> seltype = (caseExpr.selector == null) ? null : caseExpr.selector.getType();
        for (final Case c : caseExpr.cases) {
            if (i == 0) {
                resulttype = c.expr.getType();
            }
            else {
                resulttype = CompilerUtils.getCommonSuperType(resulttype, c.expr.getType());
                if (resulttype == null) {
                    this.error("incompatible expression type", c.expr);
                }
            }
            if (seltype != null) {
                seltype = CompilerUtils.getCommonSuperType(seltype, c.cond.getType());
                if (seltype == null) {
                    this.error("incompatible types of selector and case", c.cond);
                }
            }
            ++i;
        }
        final ValueExpr els = caseExpr.else_expr;
        if (els != null) {
            resulttype = CompilerUtils.getCommonSuperType(resulttype, els.getType());
            if (resulttype == null) {
                this.error("incompatible expression type", els);
            }
        }
        resulttype = CompilerUtils.getDefaultTypeIfCannotInfer(resulttype, String.class);
        if (seltype != null) {
            seltype = CompilerUtils.getDefaultTypeIfCannotInfer(seltype, String.class);
        }
        caseExpr.setType(resulttype);
        for (final Case c2 : caseExpr.cases) {
            c2.expr = this.cast(c2.expr, resulttype);
            if (seltype != null) {
                c2.cond = this.cast((ValueExpr)c2.cond, seltype);
            }
        }
        if (els != null) {
            caseExpr.else_expr = this.cast(els, resulttype);
        }
        return null;
    }
    
    private Method findMethod(final List<Class<?>> classes, final String funcName, final Class<?>[] sig) {
        boolean foundWithAnotherSignature = false;
        Method ret = null;
        for (final Class<?> klass : classes) {
            try {
                final Method me = CompilerUtils.findMethod(klass, funcName, sig);
                if (ret != null) {
                    this.error("ambiguous function call: same function exists in more than one package", funcName);
                }
                else {
                    ret = me;
                }
            }
            catch (NoSuchMethodException ex) {}
            catch (AmbiguousSignature e) {
                this.error("ambiguous function call", funcName);
            }
            catch (SignatureNotFound e2) {
                foundWithAnotherSignature = true;
            }
        }
        if (ret == null) {
            if (foundWithAnotherSignature) {
                final ArrayList<String> args = new ArrayList<String>();
                for (final Class<?> k : sig) {
                    args.add(k.getSimpleName());
                }
                this.error("no function for such arguments " + args, funcName);
            }
            else {
                this.error("no such function", funcName);
            }
        }
        return ret;
    }
    
    @Override
    public Expr visitFuncCall(final FuncCall funcCall, final AFVparams params) {
        if (funcCall.isValidated()) {
            return null;
        }
        final String funcName = funcCall.getName();
        final Class<?>[] sig = Expr.getSignature(funcCall.getFuncArgs());
        final ValueExpr fthis = funcCall.getThis();
        List<Class<?>> klasses = null;
        boolean checkStatic = false;
        if (fthis != null) {
            checkStatic = fthis.isTypeRef();
            klasses = Collections.singletonList(fthis.getType());
        }
        else {
            klasses = this.ctx.getListOfStaticMethods(funcName);
        }
        final Method me = this.findMethod(klasses, funcName, sig);
        funcCall.setMethodRef(me);
        if (checkStatic && !Modifier.isStatic(me.getModifiers())) {
            this.error("non-static method invocation", funcName);
        }
        final CustomFunctionTranslator ct = funcCall.isCustomFunc();
        final WildCardExpr wc = findWildCardExpr(funcCall.getFuncArgs());
        final Class<?>[] parameterTypes = me.getParameterTypes();
        final List<ValueExpr> argList = funcCall.getFuncArgs();
        final int numParameters = parameterTypes.length;
        final int numArguments = argList.size();
        final boolean needsVarArgsTreatment = me.isVarArgs() && (numParameters != numArguments || !parameterTypes[numParameters - 1].isAssignableFrom(argList.get(numArguments - 1).getType()));
        final ListIterator<ValueExpr> it = argList.listIterator();
        final int itCount = numParameters - (needsVarArgsTreatment ? 1 : 0);
        for (int pInd = 0; pInd < itCount; ++pInd) {
            final ValueExpr expr = it.next();
            final Class<?> paramType = parameterTypes[pInd];
            if ((wc == null && ct == null) || paramType != Object.class) {
                final ValueExpr exprCasted = this.cast(expr, paramType);
                it.set(exprCasted);
            }
        }
        if (needsVarArgsTreatment) {
            final Class<?> variableArgumentType = parameterTypes[itCount].getComponentType();
            final List<ValueExpr> exprList = new ArrayList<ValueExpr>();
            while (it.hasNext()) {
                final ValueExpr expr2 = it.next();
                final ValueExpr exprCasted = this.cast(expr2, variableArgumentType);
                exprList.add(exprCasted);
                it.remove();
            }
            argList.add(new ArrayInitializer(variableArgumentType, exprList));
        }
        if (ct != null) {
            final ValueExpr ret = ct.validate(this.ctx, funcCall);
            if (ret != null) {
                return ret;
            }
        }
        if (funcCall.getAggrDesc() != null) {
            if (!this.ctx.acceptAgg()) {
                this.error("invalid use of aggregation function", funcName);
            }
            else if (params.wasSeen()) {
                this.error("nested aggregation function", funcName);
            }
            else {
                params.wasSeenHere = true;
            }
            this.ctx.setHaveAggFuncs();
        }
        if (funcCall.isDistinct() && funcCall.getAggrClass() == null) {
            this.error("this function cannot count DISTINCT values", funcName);
        }
        if (wc != null) {
            if (this.ctx.acceptWildcard() && this.parent == null && funcCall.acceptWildcard()) {
                final List<ValueExpr> listOfFuncCall = new ArrayList<ValueExpr>();
                for (final ValueExpr arg : wc.getExprs()) {
                    final List<ValueExpr> fcargs = new ArrayList<ValueExpr>();
                    fcargs.add(this.cast(arg, Object.class));
                    final FuncCall fc = new FuncCall(funcCall.getName(), fcargs, 0);
                    fc.setMethodRef(funcCall.getMethd());
                    listOfFuncCall.add(fc);
                }
                wc.updateExprs(listOfFuncCall);
                return wc;
            }
            this.error("syntax error", wc);
        }
        return null;
    }
    
    @Override
    public Expr visitConstructorCall(final ConstructorCall constructorCall, final AFVparams params) {
        final Class<?> type = this.getTypeInfo(constructorCall.objtype);
        final Class<?>[] sig = Expr.getSignature(constructorCall.args);
        try {
            final Constructor<?> con = CompilerUtils.findConstructor(type, sig);
            constructorCall.setConstructor(con);
            final Class<?>[] parameterTypes = con.getParameterTypes();
            final List<ValueExpr> argList = constructorCall.args;
            final int numParameters = parameterTypes.length;
            final int numArguments = argList.size();
            final boolean needsVarArgsTreatment = con.isVarArgs() && (numParameters != numArguments || !parameterTypes[numParameters - 1].isAssignableFrom(argList.get(numArguments - 1).getType()));
            final ListIterator<ValueExpr> it = argList.listIterator();
            final int itCount = numParameters - (needsVarArgsTreatment ? 1 : 0);
            for (int pInd = 0; pInd < itCount; ++pInd) {
                final ValueExpr expr = it.next();
                final ValueExpr exprCasted = this.cast(expr, parameterTypes[pInd]);
                it.set(exprCasted);
            }
            if (needsVarArgsTreatment) {
                final Class<?> variableArgumentType = parameterTypes[itCount].getComponentType();
                final List<ValueExpr> exprList = new ArrayList<ValueExpr>();
                while (it.hasNext()) {
                    final ValueExpr expr2 = it.next();
                    final ValueExpr exprCasted2 = this.cast(expr2, variableArgumentType);
                    exprList.add(exprCasted2);
                    it.remove();
                }
                argList.add(new ArrayInitializer(variableArgumentType, exprList));
            }
        }
        catch (AmbiguousSignature e) {
            this.error("ambiguous constructor call", constructorCall.objtype.name);
        }
        catch (SignatureNotFound e2) {
            this.error("no such constructor", constructorCall.objtype.name);
        }
        return null;
    }
    
    @Override
    public Expr visitCastExpr(final CastExpr castExpr, final AFVparams params) {
        final Class<?> resultType = this.getTypeInfo(castExpr.casttype);
        final ValueExpr expr = castExpr.getExpr();
        final ValueExpr ret = this.cast(expr, resultType);
        return ret.setOriginalExpr(castExpr);
    }
    
    @Override
    public Expr visitFieldRef(final FieldRef fieldRef, final AFVparams params) {
        final String name = fieldRef.getName();
        final ValueExpr expr = fieldRef.getExpr();
        if (expr instanceof ObjectRef) {
            final ObjectRef o = (ObjectRef)expr;
            final StringBuilder sb = new StringBuilder();
            final ObjectRef objectRef = o;
            objectRef.name = sb.append(objectRef.name).append(".").append(name).toString();
            return o.setOriginalExpr(fieldRef);
        }
        try {
            final FieldRef f = expr.getField(name);
            if (expr.isTypeRef() && !f.isStatic()) {
                this.error("cannot access non-static field of class " + expr.getType().getName(), name);
            }
            return f.setOriginalExpr(fieldRef);
        }
        catch (NoSuchFieldException | SecurityException ex2) {
            if (JsonNode.class.isAssignableFrom(expr.getType())) {
                return new JsonFieldRef(expr, name);
            }
            this.error("cannot resolve field name", name);
            return null;
        }
    }
    
    private int getIndex(final ValueExpr index) {
        if (!(index instanceof Constant)) {
            this.error("index must be an integer constant", index);
        }
        final Constant c = (Constant)index;
        final Number n = (Number)c.value;
        return n.intValue();
    }
    
    @Override
    public Expr visitIndexExpr(final IndexExpr indexExpr, final AFVparams params) {
        ValueExpr index = indexExpr.getIndex();
        final Class<?> itype = index.getType();
        if (!CompilerUtils.isConvertibleWithBoxing(Integer.TYPE, itype)) {
            this.error("invalid type of array index", index);
        }
        final ValueExpr e = indexExpr.getExpr();
        if (e instanceof PatternVarRef) {
            final PatternVarRef pv = (PatternVarRef)e;
            pv.setIndex(this.getIndex(index));
            return pv.setOriginalExpr(indexExpr);
        }
        if (e instanceof DataSetRef) {
            final DataSetRef dsref = (DataSetRef)e;
            dsref.setIndex(this.getIndex(index));
            this.error("indexed access to a dataset is not implemented yet", indexExpr);
            return dsref.setOriginalExpr(indexExpr);
        }
        if (!indexExpr.baseIsArray()) {
            this.error("cannot apply index to expression", e);
        }
        index = this.cast(index, Integer.TYPE);
        indexExpr.setIndex(index);
        return null;
    }
    
    @Override
    public Expr visitLogicalPredicate(final LogicalPredicate logicalPredicate, final AFVparams params) {
        for (final Predicate arg : logicalPredicate.args) {
            if (arg.getType() != Boolean.TYPE) {
                this.error("invalid boolean expression", arg);
            }
        }
        return null;
    }
    
    @Override
    public Expr visitComparePredicate(final ComparePredicate comparePredicate, final AFVparams params) {
        switch (comparePredicate.op) {
            case CHECKRECTYPE: {
                break;
            }
            case ISNULL: {
                Class<?> t = comparePredicate.args.get(0).getType();
                t = CompilerUtils.getDefaultTypeIfCannotInfer(t, Object.class);
                this.castArgsTo(comparePredicate.args, t);
                break;
            }
            case LIKE: {
                this.castArgsTo(comparePredicate.args, String.class);
                final ValueExpr pat = compilePattern(comparePredicate.args.get(1));
                comparePredicate.args.set(1, pat);
                break;
            }
            case BOOLEXPR: {
                this.castArgsTo(comparePredicate.args, Boolean.TYPE);
                break;
            }
            default: {
                int i = 0;
                Class<?> commontype = null;
                for (final ValueExpr arg : comparePredicate.args) {
                    final Class<?> type = arg.getType();
                    if (!CompilerUtils.isComparable(type)) {
                        this.error("expression of uncomparable type", arg);
                    }
                    if (i == 0) {
                        commontype = type;
                    }
                    else {
                        commontype = CompilerUtils.getCommonSuperType(commontype, type);
                        if (commontype == null) {
                            this.error("expression of uncomparable type", arg);
                        }
                    }
                    ++i;
                }
                assert commontype != null;
                commontype = CompilerUtils.getDefaultTypeIfCannotInfer(commontype, String.class);
                this.castArgsTo(comparePredicate.args, commontype);
                break;
            }
        }
        return null;
    }
    
    @Override
    public Expr visitInstanceOfPredicate(final InstanceOfPredicate instanceOfPredicate, final AFVparams params) {
        Class<?> t = instanceOfPredicate.args.get(0).getType();
        t = CompilerUtils.getDefaultTypeIfCannotInfer(t, Object.class);
        this.castArgsTo(instanceOfPredicate.args, t);
        this.getTypeInfo(instanceOfPredicate.checktype);
        return null;
    }
    
    @Override
    public Expr visitClassRef(final ClassRef classRef, final AFVparams params) {
        this.getTypeInfo(classRef.typeName);
        return null;
    }
    
    @Override
    public Expr visitArrayTypeRef(final ArrayTypeRef arrayTypeRef, final AFVparams params) {
        this.error("syntax error", arrayTypeRef);
        return null;
    }
    
    @Override
    public Expr visitObjectRef(final ObjectRef objectRef, final AFVparams params) {
        final String name = objectRef.name.toString();
        final DataSet ds = this.ctx.getDataSet(name);
        if (ds != null) {
            final DataSetRef dsref = ds.makeRef();
            return dsref.setOriginalExpr(objectRef);
        }
        final FieldRef f = this.ctx.findField(name);
        if (f != null) {
            return f.setOriginalExpr(objectRef);
        }
        final Expr e = this.ctx.resolveAlias(name);
        if (e != null) {
            return e;
        }
        objectRef.setContext(this.ctx);
        return null;
    }
    
    @Override
    public Expr visitDataSetRef(final DataSetRef dataSetRef, final AFVparams params) {
        return null;
    }
    
    @Override
    public Expr visitTypeRef(final TypeRef typeRef, final AFVparams params) {
        return null;
    }
    
    @Override
    public Expr visitStaticFieldRef(final StaticFieldRef fieldRef, final AFVparams params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitCastOperation(final CastOperation castOp, final AFVparams params) {
        return null;
    }
    
    public static ValueExpr compilePattern(final ValueExpr str) {
        final List<ValueExpr> fargs = new ArrayList<ValueExpr>();
        fargs.add(str);
        final FuncCall fc = new FuncCall("compile__like__pattern", fargs, 0);
        try {
            final Method m = BuiltInFunc.class.getMethod("compile__like__pattern", String.class);
            fc.setMethodRef(m);
        }
        catch (NoSuchMethodException | SecurityException ex2) {
        		ex2.printStackTrace();
            assert false;
        }
        return fc;
    }
    
    @Override
    public Expr visitWildCardExpr(final WildCardExpr e, final AFVparams params) {
        if (this.ctx.acceptWildcard() && (this.parent == null || this.parent instanceof FuncCall)) {
            final DataSet ds = this.ctx.getDataSet(e.getDataSetName());
            if (ds == null) {
                this.error("no such data source", e);
            }
            e.setExprs(ds.makeListOfAllFields());
        }
        else {
            this.error("syntax error", e);
        }
        return null;
    }
    
    @Override
    public Expr visitParamRef(final ParamRef e, final AFVparams params) {
        return this.ctx.addParameter(e);
    }
    
    @Override
    public Expr visitPatternVarRef(final PatternVarRef e, final AFVparams params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitRowSetRef(final RowSetRef rowSetRef, final AFVparams params) {
        return null;
    }
    
    @Override
    public Expr visitArrayInitializer(final ArrayInitializer arrayInit, final AFVparams params) {
        final Class<?> compType = arrayInit.getType().getComponentType();
        for (final ValueExpr e : arrayInit.args) {
            final Class<?> type = e.getType();
            if (!compType.isAssignableFrom(type)) {
                this.error("cannot convert expression to a array component type", e);
            }
        }
        return null;
    }
    
    @Override
    public Expr visitStringConcatenation(final StringConcatenation stringConcatenation, final AFVparams params) {
        return null;
    }
    
    static class AFVparams
    {
        public final List<Boolean> seenList;
        public boolean wasSeenHere;
        
        AFVparams() {
            this.seenList = new ArrayList<Boolean>();
            this.wasSeenHere = false;
        }
        
        public boolean wasSeen() {
            for (final boolean seen : this.seenList) {
                if (seen) {
                    return true;
                }
            }
            return false;
        }
    }
}
