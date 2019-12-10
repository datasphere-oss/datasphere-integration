package com.datasphere.runtime.compiler.patternmatch;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.datasphere.classloading.BundleDefinition;
import com.datasphere.classloading.HDLoader;
import com.datasphere.event.QueryResultEvent;
import com.datasphere.proc.events.DynamicEvent;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.ParamRef;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.select.DataSet;
import com.datasphere.runtime.compiler.select.DataSets;
import com.datasphere.runtime.compiler.select.ExprGenerator;
import com.datasphere.runtime.compiler.select.Generator;
import com.datasphere.runtime.compiler.select.ParamDesc;
import com.datasphere.runtime.compiler.select.RSFieldDesc;
import com.datasphere.runtime.compiler.select.SelectCompiler;
import com.datasphere.runtime.compiler.select.TraslatedSchemaInfo;
import com.datasphere.runtime.compiler.select.Var;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.components.CQPatternMatcher;
import com.datasphere.runtime.containers.DynamicEventWrapper;
import com.datasphere.runtime.meta.CQExecutionPlan;
import com.datasphere.runtime.utils.FieldToObject;
import com.datasphere.runtime.utils.StringUtils;
import com.datasphere.uuid.UUID;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtMethod;
import javassist.CtNewMethod;
import javassist.NotFoundException;

public class MatchGenerator extends Generator
{
    private final ExprGenerator exprGen;
    private final Compiler compiler;
    private final String cqName;
    private final Select select;
    private final TraceOptions traceOptions;
    private final DataSets dataSets;
    private final Map<String, ParamRef> parameters;
    private final List<SelectCompiler.Target> targets;
    private final Class<?> targetType;
    private final boolean targetIsObj;
    private final Map<String, Integer> targetFieldIndices;
    private final List<UUID> recordedTypes;
    private final Collection<PatternVariable> variables;
    private final List<ValueExpr> partKey;
    private final ValidatedPatternNode pattern;
    private final String cqNamespace;
    private int prevEventIndex;
    
    public MatchGenerator(final Compiler compiler, final String cqName, final String cqNamespace, final Select select, final TraceOptions traceOptions, final DataSets dataSets, final Map<String, ParamRef> parameters, final List<SelectCompiler.Target> targets, final Class<?> targetType, final boolean targetIsObj, final Map<String, Integer> targetFieldIndices, final List<UUID> recordedTypes, final Collection<PatternVariable> variables, final List<ValueExpr> partKey, final ValidatedPatternNode pattern) {
        this.prevEventIndex = 0;
        this.exprGen = new ExprGenerator(compiler, true) {
            @Override
            public void setPrevEventIndex(final int index) {
                if (index > MatchGenerator.this.prevEventIndex) {
                    MatchGenerator.this.prevEventIndex = index;
                }
            }
        };
        this.compiler = compiler;
        this.cqName = cqName;
        this.select = select;
        this.traceOptions = traceOptions;
        this.dataSets = dataSets;
        this.parameters = parameters;
        this.targets = targets;
        this.targetType = targetType;
        this.targetIsObj = targetIsObj;
        this.targetFieldIndices = targetFieldIndices;
        this.recordedTypes = recordedTypes;
        this.variables = variables;
        this.partKey = partKey;
        this.pattern = pattern;
        this.cqNamespace = cqNamespace;
    }
    
    @Override
    public CQExecutionPlan generate() throws NotFoundException, CannotCompileException, IOException {
        final List<byte[]> subtasks = this.generateExecutionPlan();
        final List<CQExecutionPlan.DataSource> dataSources = this.genDataSourcesInfo();
        final List<RSFieldDesc> rsdesc = this.makeResultSetDesc();
        final List<ParamDesc> paramsDesc = this.makeParamsDesc();
        return new CQExecutionPlan(rsdesc, paramsDesc, this.select.kindOfStream, dataSources, subtasks, 0, Collections.emptyList(), this.recordedTypes, false, false, this.traceOptions, this.dataSets.size(), this.isQueryStateful(), true, null);
    }
    
    private List<ParamDesc> makeParamsDesc() {
        final List<ParamDesc> params = new ArrayList<ParamDesc>();
        for (final ParamRef r : this.parameters.values()) {
            final String pname = r.getName();
            final int pindex = r.getIndex();
            if (pindex == -1) {
                throw new RuntimeException("Index for parameter <" + pname + "> is not set");
            }
            final Class<?> ptype = r.getExpectedType();
            if (ptype == null) {
                throw new RuntimeException("Type for parameter <" + pname + "> is not indentified");
            }
            final ParamDesc d = new ParamDesc(pname, pindex, ptype);
            params.add(d);
        }
        return params;
    }
    
    private boolean isQueryStateful() {
        for (final DataSet ds : this.dataSets) {
            if (!ds.isStateful()) {
                return false;
            }
        }
        return true;
    }
    
    private List<RSFieldDesc> makeResultSetDesc() {
        final List<RSFieldDesc> fields = new ArrayList<RSFieldDesc>();
        for (final SelectCompiler.Target t : this.targets) {
            Class<?> type = t.expr.getType();
            if (this.targetType == null) {
                final Class<?> objType = CompilerUtils.getBoxingType(type);
                if (objType != null) {
                    type = objType;
                }
            }
            final RSFieldDesc f = new RSFieldDesc(t.alias, type);
            fields.add(f);
        }
        return fields;
    }
    
    private List<CQExecutionPlan.DataSource> genDataSourcesInfo() {
        final List<CQExecutionPlan.DataSource> sources = new ArrayList<CQExecutionPlan.DataSource>();
        for (final DataSet ds : this.dataSets) {
            if (ds.isIterator() == null) {
                final TraslatedSchemaInfo ts = ds.isTranslated();
                final UUID expectedTypeID = (ts == null) ? null : ts.expectedTypeID();
                final CQExecutionPlan.DataSource dsi = new CQExecutionPlan.DataSource(ds.getFullName(), ds.getUUID(), expectedTypeID);
                assert ds.getID() == sources.size();
                sources.add(dsi);
            }
        }
        return sources;
    }
    
    private List<byte[]> generateExecutionPlan() throws NotFoundException, CannotCompileException, IOException {
        final String ns = this.compiler.getContext().getCurNamespace().name;
        final HDLoader wal = HDLoader.get();
        final String uri = wal.getBundleUri(ns, BundleDefinition.Type.query, this.cqName);
        try {
            wal.lockBundle(uri);
            if (wal.isExistingBundle(uri)) {
                wal.removeBundle(uri);
            }
            wal.addBundleDefinition(ns, BundleDefinition.Type.query, this.cqName);
        }
        finally {
            wal.unlockBundle(uri);
        }
        final byte[] code = this.genSubTaskClass(wal, uri, CQPatternMatcher.class);
        return Collections.singletonList(code);
    }
    
    private byte[] genSubTaskClass(final HDLoader wal, final String bundleUri, final Class<?> superClass) throws NotFoundException, CannotCompileException, IOException {
        final List<String> methodsCode = this.genMethods(superClass);
        final ClassPool pool = wal.getBundlePool(bundleUri);
        final StringBuilder src = new StringBuilder();
        final String className = ("QueryExecPlan_" + this.cqNamespace + "_" + this.cqName).replace('.', '_');
        final CtClass cc = pool.makeClass(className);
        final CtClass sup = pool.get(superClass.getName());
        cc.setSuperclass(sup);
        src.append("public class " + className + " extends " + superClass.getName() + "\n{\n");
        this.genStaticInit(cc, src);
        this.exprGen.genVarsDeclaration(cc, src);
        for (final String mCode : methodsCode) {
            if (mCode != null) {
                this.makeMethod(mCode, cc, src);
            }
        }
        src.append("//end of " + className + "\n}\n");
        cc.setModifiers(cc.getModifiers() & 0xFFFFFBFF);
        cc.setModifiers(1);
        final boolean dumpcode = (this.traceOptions.traceFlags & 0x4) > 0;
        final boolean debugPath = this.traceOptions.traceFilePath != null;
        final byte[] code = cc.toBytecode();
        final String sourceCode = src.toString();
        if (dumpcode) {
            final PrintStream out = TraceOptions.getTraceStream(this.traceOptions);
            out.println(sourceCode);
            if (debugPath) {
                final String srcpath = this.traceOptions.traceFilePath + "/" + className + ".java";
                try (final FileOutputStream stream = new FileOutputStream(srcpath)) {
                    stream.write(sourceCode.getBytes());
                }
                CompilerUtils.compileJavaFile(srcpath);
            }
        }
        final String report = CompilerUtils.verifyBytecode(code, wal);
        wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
        if (report != null) {
            throw new Error("Internal error: invalid bytecode\n" + report);
        }
        cc.detach();
        return code;
    }
    
    private List<String> genMethods(final Class<?> superClass) {
        final List<String> methodsCode = new ArrayList<String>();
        methodsCode.add(this.genOutput(superClass));
        methodsCode.addAll(this.genVarExpr(superClass));
        methodsCode.add(this.genPartitionByKey(superClass));
        methodsCode.add(this.genCreateNode(superClass));
        methodsCode.add(this.genGetRootNodeId(superClass));
        methodsCode.add(this.genGetPrevEventIndex(superClass));
        return methodsCode;
    }
    
    private static String ctxParam() {
        return CQPatternMatcher.MatcherContext.class.getCanonicalName() + " ctx";
    }
    
    private static String curEventParam() {
        return "Object event";
    }
    
    private String genGetPrevEventIndex(final Class<?> superClass) {
        assert checkMethod("getPrevEventIndex", superClass);
        return "public int getPrevEventIndex() { return " + this.prevEventIndex + ";}\n";
    }
    
    private String genPartitionByKey(final Class<?> superClass) {
        if (this.partKey == null || this.partKey.isEmpty()) {
            return null;
        }
        this.exprGen.removeAllTmpVarsAndFrames();
        final StringBuilder body = new StringBuilder();
        final Pair<List<Var>, String> keyCode = this.genExprs(this.partKey);
        final Class<?>[] sig = Expr.getSignature(this.partKey);
        body.append(keyCode.second);
        final List<String> keyArgs = new ArrayList<String>();
        for (final Var v : keyCode.first) {
            keyArgs.add(v.getName());
        }
        int i = 0;
        for (final Class<?> t : sig) {
            if (t.isPrimitive()) {
                final Class<?> bt = CompilerUtils.getBoxingType(t);
                final String arg = keyArgs.get(i);
                final String newArg = "\n\t" + bt.getName() + ".valueOf(" + arg + ")";
                keyArgs.set(i, newArg);
            }
            ++i;
        }
        final String args = StringUtils.join((List)keyArgs);
        final String factory = RecordKey.getObjArrayKeyFactory();
        body.append("Object[] keyArgs = { " + args + " };\n");
        body.append("return " + factory + "(keyArgs);\n");
        final String keyType = RecordKey.class.getCanonicalName();
        assert checkMethod("makePartitionKey", superClass);
        return "public " + keyType + " makePartitionKey(" + curEventParam() + ")\n{\n" + body.toString() + "\n}\n";
    }
    
    private String genCreateNode(final Class<?> superClass) {
        final Map<Integer, String> cases = new TreeMap<Integer, String>();
        final MatchPatternGenerator gen = new MatchPatternGenerator() {
            private String makeNodeIdArray(final List<ValidatedPatternNode> subnodes) {
                final StringBuilder b = new StringBuilder();
                b.append("new int[] {");
                String sep = "";
                for (final ValidatedPatternNode n : subnodes) {
                    b.append(sep).append(n.getID());
                    sep = ",";
                }
                b.append("}");
                return b.toString();
            }
            
            private void addCase(final ValidatedPatternNode node, final String method, final Object... args) {
                assert checkMethod(method, superClass);
                final StringBuilder b = new StringBuilder();
                b.append(method).append("(ctx, ");
                b.append(node.getID());
                b.append(", ");
                b.append("\"");
                b.append(node.toString());
                b.append("\"");
                for (final Object arg : args) {
                    b.append(", ").append(arg);
                }
                b.append(")");
                cases.put(node.getID(), b.toString());
            }
            
            @Override
            public void emitAnchor(final ValidatedPatternNode node, final int id) {
                this.addCase(node, "createAnchor", id);
            }
            
            @Override
            public void emitVariable(final ValidatedPatternNode node, final PatternVariable var) {
                if (var.isEventVar()) {
                    this.addCase(node, "createVariable", var.getId());
                }
                else {
                    this.addCase(node, "createCondition", var.getId());
                }
            }
            
            @Override
            public void emitRepetition(final ValidatedPatternNode node, final ValidatedPatternNode subnode, final PatternRepetition rep) {
                this.addCase(node, "createRepetition", subnode.getID(), rep.mintimes, rep.maxtimes);
                subnode.emit(this);
            }
            
            @Override
            public void emitAlternation(final ValidatedPatternNode node, final List<ValidatedPatternNode> subnodes) {
                this.addCase(node, "createAlternation", this.makeNodeIdArray(subnodes));
                for (final ValidatedPatternNode n : subnodes) {
                    n.emit(this);
                }
            }
            
            @Override
            public void emitSequence(final ValidatedPatternNode node, final List<ValidatedPatternNode> subnodes) {
                this.addCase(node, "createSequence", this.makeNodeIdArray(subnodes));
                for (final ValidatedPatternNode n : subnodes) {
                    n.emit(this);
                }
            }
        };
        this.pattern.emit(gen);
        final StringBuilder body = new StringBuilder();
        body.append("switch(nodeid) {\n");
        for (final Map.Entry<Integer, String> e : cases.entrySet()) {
            final int nodeid = e.getKey();
            final String fcall = e.getValue();
            body.append("case " + nodeid + ": return " + fcall + ";\n");
        }
        body.append("default: throw new RuntimeException(\"Invalid pattern node id\" + nodeid);\n");
        body.append("}\n");
        assert checkMethod("createNode", superClass);
        return "public Object createNode(" + ctxParam() + ", int nodeid)\n{\n" + body.toString() + "\n}\n";
    }
    
    private String genGetRootNodeId(final Class<?> superClass) {
        assert checkMethod("getRootNodeId", superClass);
        return "public int getRootNodeId() { return " + this.pattern.getID() + ";}\n";
    }
    
    private List<String> genVarExpr(final Class<?> superClass) {
        final List<String> methods = new ArrayList<String>();
        final StringBuilder exprEval = new StringBuilder();
        exprEval.append("switch(exprid) {\n");
        for (final PatternVariable var : this.variables) {
            this.exprGen.removeAllTmpVarsAndFrames();
            final StringBuilder body = new StringBuilder();
            final MatchExprGenerator egen = new MatchExprGenerator() {
                private void callFunc(final String funcCall) {
                    body.append("return ctx." + funcCall + ";\n");
                }
                
                @Override
                public void emitCondition(final int id, final Predicate p) {
                    final Pair<Var, String> varAndCode = MatchGenerator.this.exprGen.genExpr(p);
                    body.append(varAndCode.second);
                    body.append("return " + varAndCode.first + ";\n");
                }
                
                @Override
                public void emitVariable(final int id, final Predicate p, final DataSet ds) {
                    body.append("if(checkDS(dsid, " + ds.getID() + ")) return false;\n");
                    final Pair<Var, String> varAndCode = MatchGenerator.this.exprGen.genExpr(p);
                    body.append(varAndCode.second);
                    body.append("return " + varAndCode.first + ";\n");
                }
                
                @Override
                public void emitCreateTimer(final int id, final long interval) {
                    this.callFunc("createTimer(" + id + ", (long)" + interval + ")");
                }
                
                @Override
                public void emitStopTimer(final int id, final int timerid) {
                    this.callFunc("stopTimer(" + timerid + ")");
                }
                
                @Override
                public void emitWaitTimer(final int id, final int timerid) {
                    this.callFunc("waitTimer(" + timerid + ", event)");
                }
            };
            var.emit(egen);
            final String code = "private boolean evalExpr" + var.getId() + "(" + ctxParam() + ", " + curEventParam() + ", int dsid, Long pos)\n{\n" + body.toString() + "\n}\n";
            methods.add(code);
            exprEval.append("case " + var.getId() + ": return evalExpr" + var.getId() + "(ctx, event, dsid, pos);\n");
        }
        exprEval.append("default: break;\n}/*end of switch*/\n");
        exprEval.append("throw new RuntimeException(\"unknown variable condition \" + exprid);\n");
        assert checkMethod("evalExpr", superClass);
        final String code2 = "public boolean evalExpr(" + ctxParam() + ", int exprid, " + curEventParam() + ", int dsid, Long pos)\n{\n" + exprEval.toString() + "\n}\n";
        methods.add(code2);
        return methods;
    }
    
    private String genOutput(final Class<?> superClass) {
        this.exprGen.removeAllTmpVarsAndFrames();
        final StringBuilder body = new StringBuilder();
        body.append("//output\n");
        if (this.targetType != null) {
            String targetTypeName = this.targetType.getName();
            if (!this.targetIsObj) {
                body.append(targetTypeName + " res = new " + targetTypeName + "(System.currentTimeMillis());\n");
                body.append("boolean setFieldFlag = (res.fieldIsSet != null);\n");
                body.append("res.allFieldsSet = " + ((this.targets.size() == this.targetFieldIndices.size()) ? "true" : "false") + ";\n");
                for (final SelectCompiler.Target t : this.targets) {
                    final Pair<Var, String> s = this.genExpr(t.expr);
                    body.append(s.second);
                    body.append("res." + t.targetField.getName() + "=" + s.first + ";\n");
                    final Integer index = this.targetFieldIndices.get(t.targetField.getName());
                    if (index != null) {
                        final int pos = index / 7;
                        final int offset = index % 7;
                        final byte bitset = (byte)(1 << offset);
                        body.append("if (setFieldFlag) res.fieldIsSet[" + pos + "] |= " + bitset + ";\n");
                    }
                }
            }
            else {
                assert this.targets.size() == 1;
                final ValueExpr te = this.targets.get(0).expr;
                final Pair<Var, String> s2 = this.genExpr(te);
                body.append(s2.second);
                if (te.getType().equals(DynamicEventWrapper.class)) {
                    targetTypeName = DynamicEvent.class.getName();
                    body.append(targetTypeName + " res = " + s2.first + ".event;\n");
                }
                else {
                    body.append(targetTypeName + " res = " + s2.first + ";\n");
                }
            }
        }
        else {
            final String targetTypeName = QueryResultEvent.class.getName();
            body.append("Object[] tmp = new Object[" + this.targets.size() + "];\n");
            int i = 0;
            for (final SelectCompiler.Target t2 : this.targets) {
                final Pair<Var, String> s3 = this.genExpr(t2.expr);
                body.append(s3.second);
                body.append("tmp[" + i + "] = " + FieldToObject.genConvert(s3.first.getName()) + ";\n");
                ++i;
            }
            body.append(targetTypeName + " res = new " + targetTypeName + "(System.currentTimeMillis());\n");
            assert checkMethod("setPayload", QueryResultEvent.class);
            body.append("res.setPayload(tmp);\n");
        }
        assert checkMethod("output", superClass);
        return "public Object output(" + ctxParam() + ", " + curEventParam() + ")\n{\n" + body.toString() + "return res;\n}\n";
    }
    
    private static boolean checkMethod(final String name, final Class<?> klass) {
        for (final Method m : klass.getMethods()) {
            if (m.getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
    
    private Pair<Var, String> genExpr(final Expr expr) {
        return this.exprGen.genExpr(expr);
    }
    
    private Pair<List<Var>, String> genExprs(final List<? extends Expr> exprs) {
        return this.exprGen.genExprs(exprs);
    }
    
    private void makeMethod(final String mCode, final CtClass cc, final StringBuilder src) {
        try {
            final CtMethod m = CtNewMethod.make(mCode, cc);
            cc.addMethod(m);
            src.append("\n").append(mCode);
        }
        catch (CannotCompileException e) {
            throw new RuntimeException("error in generated Java code\n" + mCode + "\nall generated code\n" + (Object)src, (Throwable)e);
        }
    }
    
    private void genStaticInit(final CtClass cc, final StringBuilder src) throws CannotCompileException {
        this.exprGen.genStaticVarsDeclaration(cc, src);
        final StringBuilder body = new StringBuilder();
        for (final String expr : this.exprGen.getStaticInitExprs()) {
            body.append(expr);
        }
        final String mCode = "public static void initStatic()\n{\n" + body.toString() + "return;\n}\n";
        this.makeMethod(mCode, cc, src);
    }
}
