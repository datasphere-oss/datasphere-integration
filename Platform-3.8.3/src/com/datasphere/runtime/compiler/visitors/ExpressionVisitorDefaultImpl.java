package com.datasphere.runtime.compiler.visitors;

import com.datasphere.runtime.compiler.exprs.*;

public class ExpressionVisitorDefaultImpl<T> implements ExpressionVisitor<T>
{
    @Override
    public Expr visitExpr(final Expr e, final T params) {
        return e.visit(this, params);
    }
    
    @Override
    public Expr visitExprDefault(final Expr e, final T params) {
        return null;
    }
    
    @Override
    public Expr visitNumericOperation(final NumericOperation operation, final T params) {
        return this.visitExprDefault(operation, params);
    }
    
    @Override
    public Expr visitConstant(final Constant constant, final T params) {
        return this.visitExprDefault(constant, params);
    }
    
    @Override
    public Expr visitCase(final CaseExpr caseExpr, final T params) {
        return this.visitExprDefault(caseExpr, params);
    }
    
    @Override
    public Expr visitFuncCall(final FuncCall funcCall, final T params) {
        return this.visitExprDefault(funcCall, params);
    }
    
    @Override
    public Expr visitConstructorCall(final ConstructorCall constructorCall, final T params) {
        return this.visitExprDefault(constructorCall, params);
    }
    
    @Override
    public Expr visitCastExpr(final CastExpr castExpr, final T params) {
        return this.visitExprDefault(castExpr, params);
    }
    
    @Override
    public Expr visitFieldRef(final FieldRef fieldRef, final T params) {
        return this.visitExprDefault(fieldRef, params);
    }
    
    @Override
    public Expr visitIndexExpr(final IndexExpr index, final T params) {
        return this.visitExprDefault(index, params);
    }
    
    @Override
    public Expr visitLogicalPredicate(final LogicalPredicate logicalPredicate, final T params) {
        return this.visitExprDefault(logicalPredicate, params);
    }
    
    @Override
    public Expr visitComparePredicate(final ComparePredicate comparePredicate, final T params) {
        return this.visitExprDefault(comparePredicate, params);
    }
    
    @Override
    public Expr visitInstanceOfPredicate(final InstanceOfPredicate instanceOfPredicate, final T params) {
        return this.visitExprDefault(instanceOfPredicate, params);
    }
    
    @Override
    public Expr visitClassRef(final ClassRef classRef, final T params) {
        return this.visitExprDefault(classRef, params);
    }
    
    @Override
    public Expr visitArrayTypeRef(final ArrayTypeRef arrayTypeRef, final T params) {
        return this.visitExprDefault(arrayTypeRef, params);
    }
    
    @Override
    public Expr visitObjectRef(final ObjectRef objectRef, final T params) {
        return this.visitExprDefault(objectRef, params);
    }
    
    @Override
    public Expr visitDataSetRef(final DataSetRef dataSetRef, final T params) {
        return this.visitExprDefault(dataSetRef, params);
    }
    
    @Override
    public Expr visitTypeRef(final TypeRef typeRef, final T params) {
        return this.visitExprDefault(typeRef, params);
    }
    
    @Override
    public Expr visitStaticFieldRef(final StaticFieldRef fieldRef, final T params) {
        return this.visitExprDefault(fieldRef, params);
    }
    
    @Override
    public Expr visitCastOperation(final CastOperation castOp, final T params) {
        return this.visitExprDefault(castOp, params);
    }
    
    @Override
    public Expr visitWildCardExpr(final WildCardExpr e, final T params) {
        return this.visitExprDefault(e, params);
    }
    
    @Override
    public Expr visitParamRef(final ParamRef e, final T params) {
        return this.visitExprDefault(e, params);
    }
    
    @Override
    public Expr visitPatternVarRef(final PatternVarRef e, final T params) {
        return this.visitExprDefault(e, params);
    }
    
    @Override
    public Expr visitRowSetRef(final RowSetRef e, final T params) {
        return this.visitExprDefault(e, params);
    }
    
    @Override
    public Expr visitArrayInitializer(final ArrayInitializer e, final T params) {
        return this.visitExprDefault(e, params);
    }
    
    @Override
    public Expr visitStringConcatenation(final StringConcatenation e, final T params) {
        return this.visitExprDefault(e, params);
    }
}
