package com.datasphere.runtime.compiler.patternmatch;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.exprs.CastExprFactory;
import com.datasphere.runtime.compiler.exprs.ComparePredicate;
import com.datasphere.runtime.compiler.exprs.Constant;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.compiler.exprs.FuncCall;
import com.datasphere.runtime.compiler.exprs.ObjectRef;
import com.datasphere.runtime.compiler.exprs.ParamRef;
import com.datasphere.runtime.compiler.exprs.PatternVarRef;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.select.DataSet;
import com.datasphere.runtime.compiler.select.DataSets;
import com.datasphere.runtime.compiler.select.DataSource;
import com.datasphere.runtime.compiler.select.ExprValidator;
import com.datasphere.runtime.compiler.select.SelectCompiler;
import com.datasphere.runtime.compiler.stmts.MatchClause;
import com.datasphere.runtime.compiler.stmts.SelectTarget;
import com.datasphere.runtime.compiler.visitors.ExprValidationVisitor;
import com.datasphere.runtime.compiler.visitors.FindCommonSubexpression;
import com.datasphere.runtime.exceptions.AmbiguousFieldNameException;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.Permute;

public class MatchValidator
{
    private final Map<String, ParamRef> parameters;
    private final DataSets dataSets;
    private final Compiler compiler;
    private Map<String, PatternVariable> variables;
    private int nodeCount;
    private int anchorCount;
    private int varCount;
    private final List<ValueExpr> partKey;
    private ValidatedPatternNode pattern;
    private final List<SelectCompiler.Target> targets;
    
    public MatchValidator(final Compiler compiler, final Map<String, ParamRef> parameters, final DataSets dataSets) {
        this.variables = Factory.makeNameMap();
        this.nodeCount = 0;
        this.anchorCount = 0;
        this.varCount = 0;
        this.partKey = new ArrayList<ValueExpr>();
        this.targets = new ArrayList<SelectCompiler.Target>();
        this.compiler = compiler;
        this.parameters = parameters;
        this.dataSets = dataSets;
    }
    
    private void error(final String message, final Object info) {
        this.compiler.error(message, info);
    }
    
    private int newNodeId() {
        return this.nodeCount++;
    }
    
    private int newVarId() {
        return this.varCount++;
    }
    
    public void validate(final MatchClause match, final List<DataSource> from, final List<SelectTarget> targets) {
        this.validateDefitions(match.definitions);
        this.validatePattern(match.pattern);
        this.validatePartitionBy(match.partitionkey, from);
        this.validateTargets(targets, match.partitionkey);
    }
    
    private void validateTargets(final List<SelectTarget> selTargets, final List<ValueExpr> partitionkey) {
        final DataSet ds = (partitionkey != null && !partitionkey.isEmpty()) ? this.dataSets.get(0) : null;
        for (final SelectTarget t : selTargets) {
            if (t.expr != null) {
                final ValueExpr e = (ValueExpr)this.validateVarExpr(t.expr, ds);
                final SelectCompiler.Target newt = new SelectCompiler.Target(e, t.alias);
                this.targets.add(newt);
            }
            else {
                this.error("only pattern variables can be referred", t);
            }
        }
    }
    
    private void validatePattern(final PatternNode pattern) {
        this.pattern = pattern.validate(this);
    }
    
    public ValidatedPatternNode visitRestartAnchor() {
        return ValidatedPatternNode.makeRestartAnchor(this.newNodeId(), this.anchorCount++);
    }
    
    public ValidatedPatternNode visitVariable(final String name) {
        final PatternVariable var = this.variables.get(name);
        if (var == null) {
            this.error("unknown pattern variable", name);
        }
        return ValidatedPatternNode.makePatternVariable(this.newNodeId(), var);
    }
    
    public ValidatedPatternNode visitRepetition(final PatternNode element, final PatternRepetition repetition) {
        final ValidatedPatternNode n = element.validate(this);
        return ValidatedPatternNode.makePatternRepetition(this.newNodeId(), n, repetition);
    }
    
    public ValidatedPatternNode visitComplexNode(final ComplexNodeType type, final List<PatternNode> elements) {
        final List<ValidatedPatternNode> vnodes = new ArrayList<ValidatedPatternNode>();
        for (final PatternNode n : elements) {
            final ValidatedPatternNode vn = n.validate(this);
            vnodes.add(vn);
        }
        switch (type) {
            case ALTERNATION: {
                return ValidatedPatternNode.makeAlternation(this.newNodeId(), vnodes);
            }
            case COMBINATION: {
                final List<ValidatedPatternNode> variants = new ArrayList<ValidatedPatternNode>();
                for (final List<ValidatedPatternNode> var : Permute.makePermutations(vnodes)) {
                    variants.add(ValidatedPatternNode.makeAlternation(this.newNodeId(), var));
                }
                return ValidatedPatternNode.makeAlternation(this.newNodeId(), variants);
            }
            case SEQUENCE: {
                return ValidatedPatternNode.makeSequence(this.newNodeId(), vnodes);
            }
            default: {
                throw new IllegalArgumentException();
            }
        }
    }
    
    private void validateDefitions(final List<PatternDefinition> definitions) {
        for (final PatternDefinition def : definitions) {
            final String name = def.varName;
            final String sname = def.streamName;
            final Predicate pred = def.predicate;
            if (this.variables.containsKey(name)) {
                this.error("pattern variable duplication", name);
            }
            PatternVariable var = null;
            if (sname == null) {
                var = PatternVariable.makeCondition(this.newVarId(), name, pred);
            }
            else {
                final DataSet ds = this.resolveDataSource(sname);
                if (ds != null) {
                    var = PatternVariable.makeVar(this.newVarId(), name, pred, sname, ds);
                }
                else {
                    final TimerFunc f = this.resolveTimerFunc(sname);
                    if (f != null) {
                        var = PatternVariable.makeTimerFunc(this.newVarId(), name, pred, sname, f);
                    }
                    else {
                        this.error("unknown data source", sname);
                    }
                }
            }
            this.variables.put(name, var);
        }
        for (final PatternVariable var2 : this.variables.values()) {
            var2.validate(this);
        }
    }
    
    private ValueExpr validateValueExpr(final ValueExpr e) {
        final ExprValidator ctx = new ExprValidator(this.compiler, this.parameters, false, false) {
            @Override
            public DataSet getDataSet(final String name) {
                return MatchValidator.this.dataSets.get(name);
            }
            
            @Override
            public FieldRef findField(final String name) {
                try {
                    return MatchValidator.this.dataSets.findField(name);
                }
                catch (AmbiguousFieldNameException e) {
                    this.error("ambigious field name reference", name);
                    return null;
                }
            }
            
            @Override
            public Expr resolveAlias(final String name) {
                return null;
            }
        };
        return (ValueExpr)new ExprValidationVisitor(ctx).validate(e);
    }
    
    private void validatePartitionBy(final List<ValueExpr> partitionkey, final List<DataSource> from) {
        if (partitionkey != null && !partitionkey.isEmpty()) {
            if (from.size() > 1) {
                this.error("partitioning can be used only with one data source", from);
            }
            for (final ValueExpr e : partitionkey) {
                final ValueExpr enew = this.validateValueExpr(e);
                this.partKey.add(enew);
            }
        }
    }
    
    private DataSet resolveDataSource(final String name) {
        return this.dataSets.get(name);
    }
    
    private TimerFunc resolveTimerFunc(final String name) {
        for (final TimerFunc f : TimerFunc.values()) {
            if (f.getFuncName().equalsIgnoreCase(name)) {
                return f;
            }
        }
        return null;
    }
    
    private Expr validateVarExpr(final Expr e, final DataSet ds) {
        final ExprValidator ctx = new ExprValidator(this.compiler, this.parameters, true, false) {
            @Override
            public DataSet getDataSet(final String name) {
                return null;
            }
            
            @Override
            public FieldRef findField(final String name) {
                if (ds == null) {
                    return null;
                }
                try {
                    return ds.makeFieldRef(ds.makeRef(), name);
                }
                catch (NoSuchFieldException | SecurityException ex2) {
                    return null;
                }
            }
            
            @Override
            public Expr resolveAlias(final String name) {
                final PatternVariable var = MatchValidator.this.variables.get(name);
                if (var == null) {
                    return null;
                }
                return new PatternVarRef(var);
            }
            
            @Override
            public DataSet getDefaultDataSet() {
                return ds;
            }
        };
        return new ExprValidationVisitor(ctx).validate(e);
    }
    
    public Predicate validateVarPredicate(final Predicate pred, final DataSet ds) {
        return (Predicate)this.validateVarExpr(pred, ds);
    }
    
    public Predicate validateCondtion(final Predicate pred) {
        return this.validateVarPredicate(pred, null);
    }
    
    private ValueExpr pred2val(final Predicate pred, final ExprCmd what) {
        if (pred instanceof ComparePredicate) {
            final ComparePredicate c = (ComparePredicate)pred;
            if (c.op == ExprCmd.BOOLEXPR) {
                final ValueExpr e = c.args.get(0);
                if (e.op == what) {
                    return e;
                }
            }
        }
        return null;
    }
    
    public long validateCreateTimer(final Predicate pred) {
        final Constant c = (Constant)this.pred2val(pred, ExprCmd.INTERVAL);
        if (c == null) {
            this.error("should be interval literal", pred);
        }
        return (long)c.value;
    }
    
    private int validateTimerPredicate(final Predicate pred) {
        final ObjectRef o = (ObjectRef)this.pred2val(pred, ExprCmd.VAR);
        PatternVariable var = null;
        if (o == null || (var = this.variables.get(o.name)) == null || !var.isTimer()) {
            this.error("should be name of timer variable", pred);
        }
        return var.getId();
    }
    
    public int validateStopTimer(final Predicate pred) {
        return this.validateTimerPredicate(pred);
    }
    
    public int validateWaitTimer(final Predicate pred) {
        return this.validateTimerPredicate(pred);
    }
    
    public ValidatedPatternNode getPattern() {
        return this.pattern;
    }
    
    public List<ValueExpr> getPartitionKey() {
        return this.partKey;
    }
    
    public Collection<PatternVariable> getVariables() {
        return this.variables.values();
    }
    
    public List<SelectCompiler.Target> getTargets() {
        return this.targets;
    }
    
    public void analyze() {
        final FindCommonSubexpression pfcs = this.makeCommonSubexprRewriter();
        final ListIterator<ValueExpr> it = this.partKey.listIterator();
        while (it.hasNext()) {
            final ValueExpr e = it.next();
            final ValueExpr enew = pfcs.rewrite(e);
            if (enew != null) {
                it.set(enew);
            }
        }
        final FindCommonSubexpression tfcs = this.makeCommonSubexprRewriter();
        for (final SelectCompiler.Target t : this.targets) {
            ValueExpr enew2 = tfcs.rewrite(t.expr);
            if (CompilerUtils.isParam(enew2.getType())) {
                enew2 = this.cast(enew2, String.class);
            }
            t.expr = enew2;
        }
        for (final PatternVariable var : this.variables.values()) {
            var.analyze(this);
        }
    }
    
    private ValueExpr cast(final ValueExpr e, final Class<?> targetType) {
        return CastExprFactory.cast(this.compiler, e, targetType);
    }
    
    private FindCommonSubexpression makeCommonSubexprRewriter() {
        final FindCommonSubexpression.Rewriter rewriter = new FindCommonSubexpression.Rewriter() {
            Map<Expr, Expr> exprIndex = Factory.makeLinkedMap();
            Map<Integer, Expr> eliminated = Factory.makeLinkedMap();
            List<FuncCall> aggregatedExprs = new ArrayList<FuncCall>();
            
            @Override
            public Expr rewriteExpr(final Expr e) {
                final Expr commonSubExpr = this.exprIndex.get(e);
                if (commonSubExpr == null) {
                    this.exprIndex.put(e, e);
                    return e;
                }
                if (!this.eliminated.containsKey(e.eid)) {
                    this.eliminated.put(e.eid, e);
                }
                return commonSubExpr;
            }
            
            @Override
            public void addAggrExpr(final FuncCall aggFuncCall) {
                if (!this.aggregatedExprs.contains(aggFuncCall)) {
                    final int index = this.aggregatedExprs.size();
                    this.aggregatedExprs.add(aggFuncCall);
                    aggFuncCall.setIndex(index);
                }
            }
        };
        return new FindCommonSubexpression(rewriter) {
            @Override
            public Expr visitPatternVarRef(final PatternVarRef e, final Object params) {
                if (this.isInAggFunc()) {
                    e.setIndex(-2);
                }
                return e;
            }
        };
    }
    
    public Predicate analyzePredicate(final Predicate p) {
        final FindCommonSubexpression fcs = this.makeCommonSubexprRewriter();
        final Predicate ret = fcs.rewrite(p);
        return ret;
    }
}
