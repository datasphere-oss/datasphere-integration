package com.datasphere.runtime.compiler.custom;

import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.visitors.*;
import com.datasphere.runtime.compiler.*;
import com.datasphere.runtime.compiler.exprs.*;
import java.util.*;

public class AnyAttrLike implements CustomFunctionTranslator
{
    @Override
    public ValueExpr validate(final ExprValidator ctx, final FuncCall f) {
        final List<ValueExpr> args = f.getFuncArgs();
        final ValueExpr arg = args.get(0);
        if (!(arg instanceof DataSetRef)) {
            ctx.error("function expects argument of <data source reference> type ", arg);
        }
        return null;
    }
    
    @Override
    public String generate(final ExprGenerator ctx, final FuncCall f) {
        final List<ValueExpr> args = f.getFuncArgs();
        final DataSetRef ds = (DataSetRef)args.get(0);
        final ValueExpr pat = ExprValidationVisitor.compilePattern(args.get(1));
        final List<Predicate> preds = new ArrayList<Predicate>();
        for (final FieldRef fld : ds.getDataSet().makeListOfAllFields()) {
            final ValueExpr cfld = new CastToStringOperation(fld);
            final Predicate like = AST.NewLikeExpr(cfld, false, pat);
            preds.add(like);
        }
        final Predicate p = new LogicalPredicate(ExprCmd.OR, preds);
        return ctx.getVar(p).getName();
    }
}
