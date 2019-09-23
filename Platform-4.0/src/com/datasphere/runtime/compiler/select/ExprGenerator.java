package com.datasphere.runtime.compiler.select;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import com.datasphere.runtime.Pair;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.visitors.ExprGenerationVisitor;
import com.datasphere.runtime.utils.Factory;

import javassist.CannotCompileException;
import javassist.CtClass;
import javassist.CtField;

public abstract class ExprGenerator
{
    private final Map<Integer, Var> varTable;
    private final LinkedList<GeneratedExpr> exprStack;
    private final List<Var> variables;
    private List<Var> staticVars;
    private List<String> staticInitExprs;
    private int nextVarID;
    private int nextGVarID;
    private Compiler compiler;
    private final boolean isPatternMatch;
    private int loopId;
    
    public ExprGenerator(final Compiler compiler, final boolean isPatternMatch) {
        this.varTable = Factory.makeLinkedMap();
        this.exprStack = new LinkedList<GeneratedExpr>();
        this.variables = new ArrayList<Var>();
        this.staticVars = new ArrayList<Var>();
        this.staticInitExprs = new ArrayList<String>();
        this.nextVarID = 0;
        this.nextGVarID = 0;
        this.loopId = -1;
        this.compiler = compiler;
        this.isPatternMatch = isPatternMatch;
    }
    
    public void removeAllTmpVarsAndFrames() {
        this.popFrame(0);
    }
    
    private String getBitsFieldPrefix(final boolean isStatic) {
        return isStatic ? "__gbits" : "__bits";
    }
    
    private String getBitsField(final int fieldIndex, final boolean isStatic) {
        return this.getBitsFieldPrefix(isStatic) + fieldIndex;
    }
    
    private String getBitsField(final Var v) {
        return this.getBitsField(v.getID() >> 6, v.isStatic());
    }
    
    public String setNullBit(final Var v, final boolean isNull) {
        final int bit = v.getID();
        final long mask = 1L << (bit & 0x3F);
        final String fld = this.getBitsField(v);
        final String op = isNull ? (" |= " + mask) : (" &= " + ~mask);
        return fld + op + "L;";
    }
    
    public String getNullBit(final Var v) {
        final int bit = v.getID();
        final long mask = 1L << (bit & 0x3F);
        final String fld = this.getBitsField(v);
        return "(" + fld + " & " + mask + "L)";
    }
    
    public String checkNullBits(final List<Var> args) {
        final BitSet lbs = new BitSet();
        final BitSet gbs = new BitSet();
        for (final Var v : args) {
            final int bit = v.getID();
            if (v.isStatic()) {
                gbs.set(bit);
            }
            else {
                lbs.set(bit);
            }
        }
        final StringBuilder sb = new StringBuilder("(");
        final long[] lbits = lbs.toLongArray();
        final long[] gbits = gbs.toLongArray();
        String del = "";
        for (int i = 0; i < lbits.length; ++i) {
            final long mask = lbits[i];
            if (mask != 0L) {
                final String fld = this.getBitsField(i, false);
                sb.append(del).append("(" + fld + " & " + mask + "L)");
                del = " | ";
            }
        }
        for (int i = 0; i < gbits.length; ++i) {
            final long mask = gbits[i];
            if (mask != 0L) {
                final String fld = this.getBitsField(i, true);
                sb.append(del).append("(" + fld + " & " + mask + "L)");
                del = " | ";
            }
        }
        sb.append(")");
        return sb.toString();
    }
    
    private Var makeNewStaticVar(final Class<?> type) {
        assert type != null;
        final Var v = new Var("gvar", this.nextGVarID++, type, true);
        return v;
    }
    
    private Var makeNewTmpVar(final Class<?> type) {
        assert type != null;
        if (CompilerUtils.isParam(type)) {
            throw new RuntimeException("Invalid variable type");
        }
        final Var v = new Var("var", this.nextVarID++, type, false);
        this.variables.add(v);
        return v;
    }
    
    public Var getVar(final Expr e) {
        final Var var = this.varTable.get(e.eid);
        if (var != null) {
            return var;
        }
        final Class<?> etype = e.getType();
        if (e.isConst()) {
            final Var newVar = this.makeNewStaticVar(etype);
            final ExprGenerationVisitor ee = new ExprGenerationVisitor(this, e, newVar);
            final String code = ee.exprCode();
            this.varTable.put(e.eid, newVar);
            this.addStaticVar(newVar, code);
            return newVar;
        }
        final Var newVar = this.makeNewTmpVar(etype);
        final ExprGenerationVisitor ee = new ExprGenerationVisitor(this, e, newVar);
        final String code = ee.exprCode();
        this.varTable.put(e.eid, newVar);
        this.exprStack.push(new GeneratedExpr(newVar, e, code));
        return newVar;
    }
    
    public int makeFrame() {
        return this.exprStack.size();
    }
    
    public List<GeneratedExpr> popFrame(final int label) {
        assert label >= 0;
        final LinkedList<GeneratedExpr> res = new LinkedList<GeneratedExpr>();
        while (this.exprStack.size() > label) {
            final GeneratedExpr ge = this.exprStack.pop();
            this.varTable.remove(ge.expr.eid);
            res.push(ge);
        }
        return res;
    }
    
    public String popFrameAndGetCode(final int label) {
        final StringBuilder buf = new StringBuilder();
        for (final GeneratedExpr ge : this.popFrame(label)) {
            buf.append(ge.code);
        }
        return buf.toString();
    }
    
    private List<GeneratedExpr> getFrame(final int label) {
        final int offset = this.exprStack.size() - label;
        final ListIterator<GeneratedExpr> it = this.exprStack.listIterator(offset);
        final List<GeneratedExpr> res = new ArrayList<GeneratedExpr>(offset);
        while (it.hasPrevious()) {
            final GeneratedExpr ge = it.previous();
            res.add(ge);
        }
        return res;
    }
    
    public String getFrameCode(final int label) {
        final StringBuilder buf = new StringBuilder();
        for (final GeneratedExpr ge : this.getFrame(label)) {
            buf.append(ge.code);
        }
        return buf.toString();
    }
    
    public Pair<Var, String> genExpr(final Expr expr) {
        final StringBuilder code = new StringBuilder();
        final int varFrame = this.makeFrame();
        final Var var = this.getVar(expr);
        for (final GeneratedExpr ge : this.getFrame(varFrame)) {
            code.append(ge.code);
        }
        return Pair.make(var, code.toString());
    }
    
    public Pair<List<Var>, String> genExprs(final List<? extends Expr> exprs) {
        final StringBuilder code = new StringBuilder();
        final List<Var> vars = new ArrayList<Var>(exprs.size());
        for (final Expr e : exprs) {
            final Pair<Var, String> p = this.genExpr(e);
            vars.add(p.first);
            code.append(p.second);
        }
        return Pair.make(vars, code.toString());
    }
    
    public String genAggExprs(final Expr expr) {
        final StringBuilder sb = new StringBuilder();
        final int varFrame = this.makeFrame();
        final ExprGenerationVisitor ee = new ExprGenerationVisitor(this, expr, null, ExprGenerationVisitor.WndAggCtx.UPDATE);
        final String code = ee.exprCode();
        this.exprStack.push(new GeneratedExpr(null, expr, code));
        for (final GeneratedExpr ge : this.getFrame(varFrame)) {
            sb.append(ge.code);
        }
        return sb.toString();
    }
    
    private void addStaticVar(final Var var, final String initExpr) {
        this.staticVars.add(var);
        this.staticInitExprs.add(initExpr);
    }
    
    public Var addStaticExpr(final Class<?> exprType, final String expr) {
        final Var gvar = this.makeNewStaticVar(exprType);
        final String initexpr = gvar + " = " + expr + ";\n";
        this.addStaticVar(gvar, initexpr);
        return gvar;
    }
    
    private List<Var> createBitFields(final boolean isStatic) {
        final List<Var> ret = new ArrayList<Var>();
        for (int n = ((isStatic ? this.nextGVarID : this.nextVarID) + 63) / 64, i = 0; i < n; ++i) {
            ret.add(new Var(this.getBitsFieldPrefix(isStatic), i, Long.TYPE, isStatic));
        }
        return ret;
    }
    
    private void genVars(final CtClass cc, final StringBuilder src, final Collection<Var> vars) throws CannotCompileException {
        for (final Var var : vars) {
            final String typeName = var.getType().getCanonicalName();
            final String varName = var.getName();
            final String statMod = var.isStatic() ? " static" : "";
            final String decl = String.format("private%s %-24s %s;\n", statMod, typeName, varName);
            final CtField fld = CtField.make(decl, cc);
            cc.addField(fld);
            src.append(decl);
        }
    }
    
    private <T> List<T> join(final List<T> a, final List<T> b) {
        final List<T> ret = new ArrayList<T>((Collection<? extends T>)a);
        ret.addAll((Collection<? extends T>)b);
        return ret;
    }
    
    public void genVarsDeclaration(final CtClass cc, final StringBuilder src) throws CannotCompileException {
        this.genVars(cc, src, this.join(this.createBitFields(false), this.variables));
    }
    
    public void genStaticVarsDeclaration(final CtClass cc, final StringBuilder src) throws CannotCompileException {
        this.genVars(cc, src, this.join(this.createBitFields(true), this.staticVars));
    }
    
    public List<String> getStaticInitExprs() {
        return this.staticInitExprs;
    }
    
    public String getExprText(final Expr e) {
        return this.compiler.getExprText(e);
    }
    
    public boolean isPatternMatch() {
        return this.isPatternMatch;
    }
    
    public void setLoopId(final int loopId) {
        this.loopId = loopId;
    }
    
    public int getLoopId() {
        return this.loopId;
    }
    
    public abstract void setPrevEventIndex(final int p0);
    
    private static class GeneratedExpr
    {
        public final Expr expr;
        public final String code;
        
        public GeneratedExpr(final Var var, final Expr expr, final String code) {
            this.expr = expr;
            this.code = code;
        }
    }
}
