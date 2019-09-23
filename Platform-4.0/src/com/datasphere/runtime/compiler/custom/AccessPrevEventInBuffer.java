package com.datasphere.runtime.compiler.custom;

import java.util.*;
import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.exprs.*;

public class AccessPrevEventInBuffer implements CustomFunctionTranslator
{
    @Override
    public ValueExpr validate(final ExprValidator ctx, final FuncCall f) {
        final List<ValueExpr> args = f.getFuncArgs();
        if (!args.isEmpty()) {
            final ValueExpr arg = args.get(0);
            if (arg.op != ExprCmd.INT) {
                ctx.error("must be integer constant", arg);
            }
        }
        final DataSet ds = ctx.getDefaultDataSet();
        if (ds == null) {
            ctx.error("function can be called only in expression defining pattern matching variable", f);
        }
        return new RowSetRef(f, ds);
    }
    
    @Override
    public String generate(final ExprGenerator ctx, final FuncCall f) {
        final List<ValueExpr> args = f.getFuncArgs();
        int index;
        if (!args.isEmpty()) {
            final ValueExpr arg = args.get(0);
            index = (int)((Constant)arg).value;
        }
        else {
            index = 1;
        }
        ctx.setPrevEventIndex(index);
        return "ctx.getEventAt(pos, " + index + ")";
    }
}
