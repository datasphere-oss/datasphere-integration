package com.datasphere.runtime.compiler.visitors;

import com.datasphere.runtime.compiler.exprs.*;

public interface ExpressionVisitor<T>
{
    Expr visitExpr(final Expr p0, final T p1);
    
    Expr visitExprDefault(final Expr p0, final T p1);
    
    Expr visitNumericOperation(final NumericOperation p0, final T p1);
    
    Expr visitConstant(final Constant p0, final T p1);
    
    Expr visitCase(final CaseExpr p0, final T p1);
    
    Expr visitFuncCall(final FuncCall p0, final T p1);
    
    Expr visitConstructorCall(final ConstructorCall p0, final T p1);
    
    Expr visitFieldRef(final FieldRef p0, final T p1);
    
    Expr visitIndexExpr(final IndexExpr p0, final T p1);
    
    Expr visitLogicalPredicate(final LogicalPredicate p0, final T p1);
    
    Expr visitComparePredicate(final ComparePredicate p0, final T p1);
    
    Expr visitInstanceOfPredicate(final InstanceOfPredicate p0, final T p1);
    
    Expr visitClassRef(final ClassRef p0, final T p1);
    
    Expr visitArrayTypeRef(final ArrayTypeRef p0, final T p1);
    
    Expr visitObjectRef(final ObjectRef p0, final T p1);
    
    Expr visitDataSetRef(final DataSetRef p0, final T p1);
    
    Expr visitTypeRef(final TypeRef p0, final T p1);
    
    Expr visitStaticFieldRef(final StaticFieldRef p0, final T p1);
    
    Expr visitCastExpr(final CastExpr p0, final T p1);
    
    Expr visitCastOperation(final CastOperation p0, final T p1);
    
    Expr visitWildCardExpr(final WildCardExpr p0, final T p1);
    
    Expr visitParamRef(final ParamRef p0, final T p1);
    
    Expr visitPatternVarRef(final PatternVarRef p0, final T p1);
    
    Expr visitRowSetRef(final RowSetRef p0, final T p1);
    
    Expr visitArrayInitializer(final ArrayInitializer p0, final T p1);
    
    Expr visitStringConcatenation(final StringConcatenation p0, final T p1);
}
