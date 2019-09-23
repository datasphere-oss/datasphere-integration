package com.datasphere.runtime.compiler.visitors;

import java.util.*;
import com.datasphere.runtime.compiler.*;
import org.joda.time.*;
import java.sql.*;
import com.datasphere.runtime.*;
import com.datasphere.runtime.exceptions.*;
import com.datasphere.runtime.utils.*;
import com.datasphere.runtime.compiler.custom.*;
import java.lang.reflect.*;
import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.exprs.*;

public class ExprGenerationVisitor implements ExpressionVisitor<Object>
{
    private final StringBuilder buf;
    private final ExprGenerator ctx;
    private final Var exprVar;
    private final WndAggCtx aggCtx;
    
    public ExprGenerationVisitor(final ExprGenerator ctx, final Expr e, final Var resVar) {
        this(ctx, e, resVar, WndAggCtx.GET);
    }
    
    public ExprGenerationVisitor(final ExprGenerator ctx, final Expr e, final Var resVar, final WndAggCtx aggCtx) {
        this.buf = new StringBuilder();
        this.ctx = ctx;
        this.exprVar = resVar;
        this.aggCtx = aggCtx;
        e.visit(this, (Object)null);
    }
    
    private Var getVar(final Expr e) {
        return this.ctx.getVar(e);
    }
    
    private int makeFrame() {
        return this.ctx.makeFrame();
    }
    
    private void popFrame(final int label) {
        this.ctx.popFrame(label);
    }
    
    private String getExprText(final Expr e) {
        return this.ctx.getExprText(e);
    }
    
    private List<Var> getArgs(final List<? extends Expr> args) {
        final List<Var> res = new ArrayList<Var>();
        for (final Expr arg : args) {
            final Var var = this.getVar(arg);
            res.add(var);
        }
        return res;
    }
    
    private String join(final List<Var> list) {
        return StringUtils.join((List)list);
    }
    
    private void popAndAppendFrame(final int label) {
        final String code = this.ctx.popFrameAndGetCode(label);
        this.buf.append(code);
    }
    
    private void getAndAppendFrame(final int label) {
        final String code = this.ctx.getFrameCode(label);
        this.buf.append(code);
    }
    
    public String exprCode() {
        return this.buf.toString();
    }
    
    private String getOp(final ExprCmd op) {
        switch (op) {
            case UMINUS: {
                return "-";
            }
            case UPLUS: {
                return "";
            }
            case PLUS: {
                return "+";
            }
            case MINUS: {
                return "-";
            }
            case DIV: {
                return "/";
            }
            case MUL: {
                return "*";
            }
            case MOD: {
                return "%";
            }
            case BITAND: {
                return "&";
            }
            case BITOR: {
                return "|";
            }
            case BITXOR: {
                return "^";
            }
            case INVERT: {
                return "~";
            }
            case LSHIFT: {
                return "<<";
            }
            case RSHIFT: {
                return ">>";
            }
            case URSHIFT: {
                return ">>>";
            }
            default: {
                assert false;
                return null;
            }
        }
    }
    
    private String getCmpOp(final ExprCmd op) {
        switch (op) {
            case EQ: {
                return "==";
            }
            case NOTEQ: {
                return "!=";
            }
            case GT: {
                return ">";
            }
            case GTEQ: {
                return ">=";
            }
            case LT: {
                return "<";
            }
            case LTEQ: {
                return "<=";
            }
            default: {
                assert false;
                return null;
            }
        }
    }
    
    private String addCheckNotNull(final Var arg1, final Var arg2, final String cmpExpr) {
        return "(" + arg1 + " != null && " + arg2 + " != null && " + cmpExpr + ")";
    }
    
    private String addCheckNull(final Var arg1, final Var arg2, final String cmpExpr) {
        return "(" + arg1 + " == null || " + arg2 + " == null || " + cmpExpr + ")";
    }
    
    private String genCompare(final ExprCmd relop, final Var arg1, final Var arg2, final Expr expr1, final Expr expr2) {
        final Class<?> t1 = expr1.getType();
        final Class<?> t2 = expr2.getType();
        assert t1.equals(t2);
        if (t1.isPrimitive()) {
            return arg1 + this.getCmpOp(relop) + arg2;
        }
        if (relop == ExprCmd.EQ) {
            return this.addCheckNotNull(arg1, arg2, arg1 + ".equals(" + arg2 + ")");
        }
        if (relop == ExprCmd.NOTEQ) {
            return this.addCheckNull(arg1, arg2, "(!" + arg1 + ".equals(" + arg2 + "))");
        }
        assert CompilerUtils.isComparable(t1);
        return this.addCheckNotNull(arg1, arg2, arg1 + ".compareTo(" + arg2 + ") " + this.getCmpOp(relop) + " 0");
    }
    
    private String setNullBit(final Var res, final List<Var> args) {
        return "if(" + this.ctx.checkNullBits(args) + " != 0) { " + this.setNullBit(res) + " }";
    }
    
    @Override
    public Expr visitNumericOperation(final NumericOperation operation, final Object params) {
        final List<Var> args = this.getArgs(operation.args);
        final String op = this.getOp(operation.op);
        if (args.size() > 1) {
            final StringBuilder sb = new StringBuilder();
            String sep = "";
            final String newsep = " " + op + " ";
            for (final Var arg : args) {
                sb.append(sep).append(arg);
                sep = newsep;
            }
            this.buf.append(this.exprVar + " = " + (Object)sb + ";\n");
        }
        else {
            assert args.size() == 1;
            this.buf.append(this.exprVar + " = " + op + " " + args.get(0) + ";\n");
        }
        this.buf.append(this.setNullBit(this.exprVar, args) + "\n");
        return null;
    }
    
    private static String convertStringToJavaLiteral(final String s) {
        return "\"" + s.replace("\n", "\\n").replace("\"", "\\\"") + "\"";
    }
    
    @Override
    public Expr visitConstant(final Constant constant, final Object params) {
        String value;
        if (constant.value == null) {
            assert constant.op == ExprCmd.NULL;
            value = "null";
        }
        else if (constant.value instanceof String) {
            value = convertStringToJavaLiteral((String)constant.value);
        }
        else if (constant.value instanceof DateTime) {
            final DateTime d = (DateTime)constant.value;
            value = "new " + DateTime.class.getName() + "(" + d.getMillis() + "L)";
        }
        else if (constant.value instanceof Timestamp) {
            final Timestamp t = (Timestamp)constant.value;
            value = BuiltInFunc.class.getName() + ".toTimestamp(" + t.getTime() + "L," + t.getNanos() + ")";
        }
        else if (constant.value instanceof Class) {
            value = ((Class)constant.value).getCanonicalName() + ".class";
        }
        else {
            value = constant.value.toString();
        }
        this.buf.append(this.exprVar + " = " + value + ";\n");
        return null;
    }
    
    @Override
    public Expr visitCase(final CaseExpr caseExpr, final Object params) {
        this.buf.append("while(true) {\n");
        final Var selector = (caseExpr.selector == null) ? null : this.getVar(caseExpr.selector);
        int i = 0;
        int caseFrame = this.makeFrame();
        for (final Case c : caseExpr.cases) {
            final int condFrame = this.makeFrame();
            final Var cond = this.getVar(c.cond);
            if (i == 0) {
                caseFrame = this.makeFrame();
            }
            else {
                this.getAndAppendFrame(condFrame);
            }
            String condExpr;
            if (caseExpr.selector == null) {
                condExpr = cond.getName();
            }
            else {
                condExpr = this.genCompare(ExprCmd.EQ, cond, selector, c.cond, caseExpr.selector);
            }
            this.buf.append("if(" + condExpr + ") {\n");
            final int exprFrame = this.makeFrame();
            final Var expr = this.getVar(c.expr);
            this.popAndAppendFrame(exprFrame);
            this.buf.append(this.exprVar + " = " + expr + ";\n");
            this.buf.append("break;\n");
            this.buf.append("}\n");
            ++i;
        }
        if (caseExpr.else_expr != null) {
            final int exprFrame2 = this.makeFrame();
            final Var expr2 = this.getVar(caseExpr.else_expr);
            this.getAndAppendFrame(exprFrame2);
            this.buf.append(this.exprVar + " = " + expr2 + ";\n");
            this.buf.append("break;\n");
        }
        else {
            this.buf.append("caseNotFound();");
            this.buf.append("break;\n");
        }
        this.buf.append("}\n");
        this.popFrame(caseFrame);
        return null;
    }
    
    private void visitAggFunc(final FuncCall funcCall, final AggHandlerDesc desc) {
        final int aggExprIndex = funcCall.getIndex();
        assert aggExprIndex >= 0;
        final String obj = "((" + funcCall.getAggrClass().getCanonicalName() + ")getAggVec(" + aggExprIndex + "))";
        if (this.aggCtx == WndAggCtx.GET) {
            final String methodName = desc.getMethod();
            this.buf.append(this.exprVar + " = " + obj + "." + methodName + "();\n");
        }
        else {
            assert this.aggCtx == WndAggCtx.UPDATE;
            final String args = this.join(this.getArgs(funcCall.getFuncArgs()));
            final String incMethodName = desc.incMethod();
            final String decMethodName = desc.decMethod();
            this.buf.append("if(isAdd())\n");
            this.buf.append("\t" + obj + "." + incMethodName + "(" + args + ");\n");
            this.buf.append("else\n");
            this.buf.append("\t" + obj + "." + decMethodName + "(" + args + ");\n");
        }
    }
    
    private void visitAggFuncInMatch(final FuncCall funcCall, final AggHandlerDesc desc) {
        final int aggExprIndex = funcCall.getIndex();
        assert aggExprIndex >= 0;
        final String aggt = funcCall.getAggrClass().getCanonicalName();
        final String obj = "agg" + aggExprIndex;
        final String iter = "i" + aggExprIndex;
        this.buf.append(aggt + " " + obj + " = new " + aggt + "();\n");
        this.buf.append("int " + iter + " = 0;\n");
        this.buf.append("try {\n");
        this.buf.append("while(true) {\n");
        this.buf.append("\tboolean allNulls = true;\n");
        this.ctx.setLoopId(aggExprIndex);
        final int frame = this.makeFrame();
        final String args = this.join(this.getArgs(funcCall.getFuncArgs()));
        this.popAndAppendFrame(frame);
        this.ctx.setLoopId(-1);
        this.buf.append("if(allNulls) throw new " + EndOfAggLoop.class.getCanonicalName() + "();\n");
        final String incMethodName = desc.incMethod();
        this.buf.append("\t" + obj + "." + incMethodName + "(" + args + ");\n");
        this.buf.append(iter + "++;\n");
        this.buf.append("}/*while(true)*/\n");
        this.buf.append("} catch (" + EndOfAggLoop.class.getCanonicalName() + " e) {}\n");
        final String getName = desc.getMethod();
        this.buf.append(this.exprVar + " = " + obj + "." + getName + "();\n");
    }
    
    @Override
    public Expr visitFuncCall(final FuncCall funcCall, final Object params) {
        final Method me = funcCall.getMethd();
        assert me != null;
        assert NamePolicy.isEqual(me.getName(), funcCall.getName());
        final AggHandlerDesc desc = funcCall.getAggrDesc();
        if (desc != null) {
            if (this.ctx.isPatternMatch()) {
                this.visitAggFuncInMatch(funcCall, desc);
            }
            else {
                this.visitAggFunc(funcCall, desc);
            }
        }
        else {
            final CustomFunctionTranslator ct = funcCall.isCustomFunc();
            String callExpr;
            if (ct != null) {
                callExpr = ct.generate(this.ctx, funcCall);
            }
            else {
                final String args = this.join(this.getArgs(funcCall.getFuncArgs()));
                final String methodName = me.getName();
                final Expr fthis = funcCall.getThis();
                String obj;
                if (fthis == null) {
                    obj = me.getDeclaringClass().getName();
                }
                else if (fthis.isTypeRef()) {
                    obj = fthis.getType().getName();
                }
                else {
                    obj = this.getVar(fthis).getName();
                }
                callExpr = obj + "." + methodName + "(" + args + ")";
            }
            this.buf.append(this.exprVar + " = " + callExpr + ";\n");
        }
        return null;
    }
    
    @Override
    public Expr visitConstructorCall(final ConstructorCall constructorCall, final Object params) {
        final String args = this.join(this.getArgs(constructorCall.args));
        this.buf.append(this.exprVar + " = new " + constructorCall.objtype + "(" + args + ");\n");
        return null;
    }
    
    @Override
    public Expr visitCastExpr(final CastExpr castExpr, final Object params) {
        assert false;
        return null;
    }
    
    private void appendCheckNull(final String obj, final String name) {
        this.buf.append("if(" + obj + " == null) { ");
        this.buf.append("throw new RuntimeException(\"accessed field " + name + " from null object\"); }\n");
    }
    
    @Override
    public Expr visitFieldRef(final FieldRef fieldRef, final Object params) {
        final Var obj = this.getVar(fieldRef.getExpr());
        final String fldexpr = fieldRef.genFieldAccess(obj.getName());
        final Class<?> ft = fieldRef.getType();
        this.buf.append("if(" + obj + " == null) {\n");
        if (ft.isPrimitive()) {
            this.buf.append("throw new NullPointerException(\"cannot unreference field of primitive type <" + fieldRef.getName() + ">with null value\");\n");
        }
        else {
            this.buf.append(this.exprVar + " = null;\n");
        }
        this.buf.append("} else {\n");
        this.buf.append(this.exprVar + " = " + fldexpr + ";\n");
        this.buf.append("}\n");
        return null;
    }
    
    @Override
    public Expr visitIndexExpr(final IndexExpr indexExpr, final Object params) {
        final Var index = this.getVar(indexExpr.getIndex());
        final Var array = this.getVar(indexExpr.getExpr());
        final String expr = this.getExprText(indexExpr);
        this.appendCheckNull(array.getName(), expr);
        if (indexExpr.baseIsJsonNode()) {
            this.buf.append(this.exprVar + " = " + array + ".get(" + index + ");\n");
        }
        else {
            this.buf.append("try { " + this.exprVar + " = " + array + "[" + index + "]; } ");
            this.buf.append("catch(java.lang.ArrayIndexOutOfBoundsException e) { ");
            this.buf.append("throw new RuntimeException(\"in expression <" + expr + "> trying to access \" + " + index + "+\"-th element of array, while it has only \" +" + array + ".length + \" elements\", e);");
            this.buf.append("}\n");
        }
        return null;
    }
    
    @Override
    public Expr visitLogicalPredicate(final LogicalPredicate logicalPredicate, final Object params) {
        final ExprCmd op = logicalPredicate.op;
        if (op == ExprCmd.NOT) {
            assert logicalPredicate.args.size() == 1;
            final Var arg = this.getVar(logicalPredicate.args.get(0));
            this.buf.append(this.exprVar + " = !" + arg + ";\n");
        }
        else {
            assert op == ExprCmd.OR;
            this.buf.append(this.exprVar + " = " + ((op == ExprCmd.AND) ? "false" : "true") + ";\n");
            this.buf.append("while(true) {\n");
            int i = 0;
            int exprFrame = this.makeFrame();
            for (final Predicate p : logicalPredicate.args) {
                final int condFrame = this.makeFrame();
                final Var cond = this.getVar(p);
                if (i == 0) {
                    exprFrame = this.makeFrame();
                }
                else {
                    this.getAndAppendFrame(condFrame);
                }
                if (op == ExprCmd.AND) {
                    this.buf.append("if(!(" + cond + ")) {\n");
                }
                else {
                    this.buf.append("if(" + cond + ") {\n");
                }
                this.buf.append("break;\n");
                this.buf.append("}\n");
                ++i;
            }
            this.buf.append(this.exprVar + " = " + ((op == ExprCmd.AND) ? "true" : "false") + ";\n");
            this.buf.append("break;\n");
            this.buf.append("}\n");
            this.popFrame(exprFrame);
        }
        return null;
    }
    
    private void visitInListPredicate(final ComparePredicate comparePredicate) {
        assert comparePredicate.op == ExprCmd.INLIST;
        final List<ValueExpr> list = comparePredicate.args;
        final Expr firstExpr = list.get(0);
        final Var arg = this.getVar(firstExpr);
        this.buf.append(this.exprVar + " = true;\n");
        this.buf.append("while(true) {\n");
        int i = 0;
        int listFrame = this.makeFrame();
        for (final ValueExpr e : list.subList(1, list.size())) {
            final int condFrame = this.makeFrame();
            final Var cond = this.getVar(e);
            if (i == 0) {
                listFrame = this.makeFrame();
            }
            else {
                this.getAndAppendFrame(condFrame);
            }
            final String condExpr = this.genCompare(ExprCmd.EQ, cond, arg, firstExpr, e);
            this.buf.append("if(" + condExpr + ") {\n");
            this.buf.append("break;\n");
            this.buf.append("}\n");
            ++i;
        }
        this.buf.append(this.exprVar + " = false;\n");
        this.buf.append("break;\n");
        this.buf.append("}\n");
        this.popFrame(listFrame);
    }
    
    @Override
    public Expr visitComparePredicate(final ComparePredicate comparePredicate, final Object params) {
        switch (comparePredicate.op) {
            case BOOLEXPR: {
                final Var arg = this.getVar(comparePredicate.args.get(0));
                this.buf.append(this.exprVar + " = " + arg + ";\n");
                break;
            }
            case CHECKRECTYPE: {
                final Var arg = this.getVar(comparePredicate.args.get(0));
                this.buf.append(this.exprVar + " = checkDynamicRecType(" + arg + ");\n");
                break;
            }
            case ISNULL: {
                final ValueExpr e = comparePredicate.args.get(0);
                if (e.getType().isPrimitive()) {
                    this.buf.append(this.exprVar + " = false;\n");
                    break;
                }
                final Var arg2 = this.getVar(e);
                this.buf.append(this.exprVar + " = " + arg2 + " == null;\n");
                break;
            }
            case LIKE: {
                final String args = this.join(this.getArgs(comparePredicate.args));
                this.buf.append(this.exprVar + " = doLike(" + args + ");\n");
                break;
            }
            case INLIST: {
                this.visitInListPredicate(comparePredicate);
                break;
            }
            case BETWEEN: {
                final Expr argExpr = comparePredicate.args.get(0);
                final Expr lbExpr = comparePredicate.args.get(1);
                final Expr ubExpr = comparePredicate.args.get(2);
                final Var arg3 = this.getVar(argExpr);
                final Var lowerbound = this.getVar(lbExpr);
                final String condExpr1 = this.genCompare(ExprCmd.LT, arg3, lowerbound, argExpr, lbExpr);
                this.buf.append("if(" + condExpr1 + ") {\n");
                this.buf.append(this.exprVar + " = false;\n");
                this.buf.append("} else {\n");
                final int frame = this.makeFrame();
                final Var upperbound = this.getVar(ubExpr);
                this.popAndAppendFrame(frame);
                final String condExpr2 = this.genCompare(ExprCmd.GT, arg3, upperbound, argExpr, ubExpr);
                this.buf.append("if(" + condExpr2 + ") {\n");
                this.buf.append(this.exprVar + " = false;\n");
                this.buf.append("} else {\n");
                this.buf.append(this.exprVar + " = true;\n");
                this.buf.append("}\n");
                this.buf.append("}\n");
                break;
            }
            default: {
                final Expr arg4 = comparePredicate.args.get(0);
                final Expr arg5 = comparePredicate.args.get(1);
                final List<Var> args2 = this.getArgs(comparePredicate.args);
                final String condExpr3 = this.genCompare(comparePredicate.op, args2.get(0), args2.get(1), arg4, arg5);
                this.buf.append(this.exprVar + " = " + condExpr3 + ";\n");
                break;
            }
        }
        return null;
    }
    
    @Override
    public Expr visitInstanceOfPredicate(final InstanceOfPredicate instanceOfPredicate, final Object Params) {
        final Var arg = this.getVar(instanceOfPredicate.getExpr());
        this.buf.append(this.exprVar + " = " + arg + " instanceof " + instanceOfPredicate.checktype + ";\n");
        return null;
    }
    
    @Override
    public Expr visitExpr(final Expr e, final Object params) {
        return e.visit(this, params);
    }
    
    @Override
    public Expr visitExprDefault(final Expr e, final Object params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitClassRef(final ClassRef classRef, final Object params) {
        this.buf.append(this.exprVar + " = " + classRef.typeName + ".class;\n");
        return null;
    }
    
    @Override
    public Expr visitArrayTypeRef(final ArrayTypeRef arrayTypeRef, final Object params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitObjectRef(final ObjectRef objectRef, final Object params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitTypeRef(final TypeRef typeRef, final Object params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitStaticFieldRef(final StaticFieldRef fieldRef, final Object params) {
        final Field f = fieldRef.fieldRef;
        final Class<?> c = f.getDeclaringClass();
        this.buf.append(this.exprVar + " = " + c.getName() + "." + f.getName() + ";\n");
        return null;
    }
    
    private static String convertToStringLiteral(final String s) {
        return (s == null) ? "" : s.replace("\"", "\\\"");
    }
    
    private static String defaultTypeValue(final Class<?> type) {
        return (type == Boolean.TYPE) ? "false" : "0";
    }
    
    private String setNullBit(final Var var) {
        return this.ctx.setNullBit(var, true);
    }
    
    private String clearNullBit(final Var var) {
        return this.ctx.setNullBit(var, false);
    }
    
    private String getNullBit(final Var v) {
        return this.ctx.getNullBit(v);
    }
    
    @Override
    public Expr visitCastOperation(final CastOperation castOp, final Object params) {
        final String type = castOp.getType().getCanonicalName();
        if (castOp instanceof CastNull) {
            this.buf.append(this.exprVar + " = (" + type + ")null;\n");
        }
        else if (castOp instanceof CastParam) {
            final ParamRef p = (ParamRef)castOp.getExpr();
            this.buf.append(this.exprVar + " = (" + type + ")getParamVal(" + p.getIndex() + ",\"" + p.getName() + "\", " + type + ".class);\n");
        }
        else {
            final ValueExpr expr = castOp.getExpr();
            final Var arg = this.getVar(expr);
            if (castOp instanceof CastToStringOperation) {
                this.buf.append(this.exprVar + " = String.valueOf(" + arg + ");\n");
            }
            else if (castOp instanceof UnboxCastOperation) {
                final String val = defaultTypeValue(castOp.getType());
                this.buf.append("if(" + arg + " == null) { ");
                this.buf.append(this.exprVar + " = " + val + ";");
                this.buf.append(this.setNullBit(this.exprVar) + " }\n");
                this.buf.append("else {");
                this.buf.append(this.exprVar + " = " + arg + "." + type + "Value();");
                this.buf.append(this.clearNullBit(this.exprVar) + "}\n");
            }
            else if (castOp instanceof BoxCastOperation) {
                this.buf.append("if(" + this.getNullBit(arg) + " != 0) { " + this.exprVar + " = null; }\n");
                this.buf.append("else {" + this.exprVar + " = " + type + ".valueOf(" + arg + "); }\n");
            }
            else if (castOp instanceof UnboxBoxCastOperation) {
                final String unboxedSrcType = CompilerUtils.getUnboxingType(expr.getType()).getCanonicalName();
                final String unboxedTgtType = CompilerUtils.getUnboxingType(castOp.getType()).getCanonicalName();
                this.buf.append("if(" + arg + " == null) { " + this.exprVar + " = null; }\n");
                this.buf.append("else { " + this.exprVar + " = " + type + ".valueOf((" + unboxedTgtType + ")(" + arg + ")." + unboxedSrcType + "Value()); }\n");
            }
            else {
                final Class<?> targetType = castOp.getType();
                final Class<?> sourceType = expr.getType();
                if ((sourceType.isPrimitive() && targetType.isPrimitive()) || targetType.isAssignableFrom(sourceType)) {
                    this.buf.append(this.exprVar + " = ((" + type + ")" + arg + ");\n");
                }
                else {
                    final String exprText = convertToStringLiteral(this.getExprText(expr));
                    this.buf.append("try { " + this.exprVar + " = ((" + type + ")" + arg + "); } ");
                    this.buf.append("catch(java.lang.ClassCastException e) { throw new ClassCastException(\"cannot cast expression <" + exprText + "> of type [\" + " + arg + ".getClass().getCanonicalName() + \"] to type [" + type + "]\"); }\n");
                }
            }
        }
        return null;
    }
    
    @Override
    public Expr visitWildCardExpr(final WildCardExpr e, final Object params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitParamRef(final ParamRef e, final Object params) {
        assert false;
        return null;
    }
    
    @Override
    public Expr visitPatternVarRef(final PatternVarRef e, final Object params) {
        final int loopid = this.ctx.getLoopId();
        final String vartype = e.getType().getCanonicalName();
        final String idx = (loopid >= 0) ? ("i" + loopid) : String.valueOf(e.getIndex());
        this.buf.append(this.exprVar + " = (" + vartype + ")ctx.getVar(" + e.getVarId() + ", " + idx + ");\n");
        if (loopid >= 0) {
            this.buf.append("if(" + this.exprVar + " != null) allNulls = false;\n");
        }
        return null;
    }
    
    private void emitRowSetRef(final RowSet rs, final String obj) {
        final String row = rs.genRowAccessor(obj);
        this.buf.append(this.exprVar + " = (" + rs.getTypeName() + ")" + row + ";\n");
    }
    
    @Override
    public Expr visitDataSetRef(final DataSetRef ref, final Object params) {
        final DataSet ds = ref.getDataSet();
        final String obj = this.ctx.isPatternMatch() ? "event" : ("getRowData(" + ds.getID() + ")");
        this.buf.append("// get row from " + ds.getFullName() + "\n");
        this.emitRowSetRef(ds, obj);
        return null;
    }
    
    @Override
    public Expr visitRowSetRef(final RowSetRef ref, final Object params) {
        final RowSet rs = ref.getRowSet();
        final Var obj = this.getVar(ref.getExpr());
        this.emitRowSetRef(rs, obj.getName());
        return null;
    }
    
    @Override
    public Expr visitArrayInitializer(final ArrayInitializer arrayInit, final Object params) {
        final String type = arrayInit.getType().getComponentType().getCanonicalName();
        final String initialValues = this.join(this.getArgs(arrayInit.args));
        this.buf.append(this.exprVar + " = new " + type + "[]{" + initialValues + "};\n");
        return null;
    }
    
    @Override
    public Expr visitStringConcatenation(final StringConcatenation expr, final Object params) {
        final List<Var> args = this.getArgs(expr.args);
        final StringBuilder sb = new StringBuilder();
        sb.append("\"\"");
        for (final Var arg : args) {
            sb.append(" + " + arg);
        }
        this.buf.append(this.exprVar + " = " + (Object)sb + ";\n");
        return null;
    }
    
    public enum WndAggCtx
    {
        UPDATE, 
        GET;
    }
}
