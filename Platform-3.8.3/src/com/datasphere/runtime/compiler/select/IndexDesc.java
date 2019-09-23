package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.compiler.exprs.*;
import java.util.*;

public class IndexDesc
{
    public final int id;
    public final DataSet dataset;
    public final List<Expr> exprs;
    
    public IndexDesc(final int id, final DataSet dataset, final List<Expr> exprs) {
        this.id = id;
        this.dataset = dataset;
        this.exprs = exprs;
    }
    
    public Class<?>[] getIndexKeySignature() {
        return Expr.getSignature(this.exprs);
    }
    
    @Override
    public String toString() {
        return "IndexDesc(" + this.id + " " + this.dataset.getName() + " " + this.exprs + ")";
    }
}
