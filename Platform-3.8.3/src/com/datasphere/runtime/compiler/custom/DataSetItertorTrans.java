package com.datasphere.runtime.compiler.custom;

import com.datasphere.runtime.compiler.exprs.*;
import java.util.*;
import com.datasphere.runtime.compiler.select.*;

public class DataSetItertorTrans implements CustomFunctionTranslator
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
        final DataSetRef dsref = (DataSetRef)f.getFuncArgs().get(0);
        return "getContext().makeSnapshotIterator(" + dsref.getDataSet().getID() + ")";
    }
}
