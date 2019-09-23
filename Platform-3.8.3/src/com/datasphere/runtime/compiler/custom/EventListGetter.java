package com.datasphere.runtime.compiler.custom;

import com.datasphere.runtime.compiler.exprs.*;
import java.util.*;
import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.*;

public class EventListGetter implements CustomFunctionTranslator
{
    @Override
    public ValueExpr validate(final ExprValidator ctx, final FuncCall f) {
        final List<ValueExpr> args = f.getFuncArgs();
        final ValueExpr arg = args.get(0);
        if (!(arg instanceof DataSetRef)) {
            ctx.error("function expects argument of <data source reference> type ", arg);
        }
        final DataSetRef ds = (DataSetRef)arg;
        if (ds.getDataSet().getKind() != DataSet.Kind.WASTORE_JAVA_OBJECTS) {
            ctx.error("function expects argument of <HD store reference> type ", arg);
        }
        return null;
    }
    
    @Override
    public String generate(final ExprGenerator ctx, final FuncCall f) {
        final DataSetRef dsref = (DataSetRef)f.getFuncArgs().get(0);
        return BuiltInFunc.class.getName() + ".eventList(getRowData(" + dsref.getDataSet().getID() + "))";
    }
}
