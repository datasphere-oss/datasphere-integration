package com.datasphere.runtime.compiler.stmts;

import java.util.*;
import com.datasphere.runtime.compiler.patternmatch.*;
import com.datasphere.runtime.compiler.exprs.*;

public class MatchClause
{
    public final PatternNode pattern;
    public final List<PatternDefinition> definitions;
    public final List<ValueExpr> partitionkey;
    
    public MatchClause(final PatternNode pattern, final List<PatternDefinition> definitions, final List<ValueExpr> partitionkey) {
        this.pattern = pattern;
        this.definitions = definitions;
        this.partitionkey = partitionkey;
    }
    
    @Override
    public String toString() {
        return "MATCH " + this.pattern + "\nDEFINE " + this.definitions + ((this.partitionkey != null) ? ("\nPARTITION BY " + this.partitionkey) : "");
    }
}
