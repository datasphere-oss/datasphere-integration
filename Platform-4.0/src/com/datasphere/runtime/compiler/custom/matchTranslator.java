package com.datasphere.runtime.compiler.custom;

import com.datasphere.runtime.compiler.exprs.*;
import com.datasphere.runtime.*;
import java.util.regex.*;
import java.util.*;
import com.datasphere.runtime.compiler.select.*;

public class matchTranslator implements CustomFunctionTranslator
{
    @Override
    public ValueExpr validate(final ExprValidator ctx, final FuncCall f) {
        return null;
    }
    
    @Override
    public String generate(final ExprGenerator ctx, final FuncCall f) {
        final List<ValueExpr> args = f.getFuncArgs();
        final Var arg1 = ctx.getVar(args.get(0));
        final ValueExpr e = args.get(1);
        Var arg2 = ctx.getVar(e);
        final String cn = BuiltInFunc.class.getName();
        if (e.isConst()) {
            final String pt = Pattern.class.getName();
            arg2 = ctx.addStaticExpr(Pattern.class, pt + ".compile(" + arg2 + ")");
        }
        return cn + ".match2impl(" + arg1 + ", " + arg2 + ")";
    }
}
