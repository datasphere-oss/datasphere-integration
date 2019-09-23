package com.datasphere.runtime.compiler.custom;

import com.datasphere.runtime.compiler.exprs.*;
import com.datasphere.runtime.compiler.select.*;

public interface CustomFunctionTranslator
{
    ValueExpr validate(final ExprValidator p0, final FuncCall p1);
    
    String generate(final ExprGenerator p0, final FuncCall p1);
}
