package com.datasphere.runtime.compiler.select;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.datasphere.classloading.BundleDefinition;
import com.datasphere.classloading.WALoader;
import com.datasphere.event.QueryResultEvent;
import com.datasphere.proc.events.DynamicEvent;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.RecordKey;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.FuncCall;
import com.datasphere.runtime.compiler.exprs.ModifyExpr;
import com.datasphere.runtime.compiler.exprs.ParamRef;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.stmts.LimitClause;
import com.datasphere.runtime.compiler.stmts.OrderByItem;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.components.CQSubTaskJoin;
import com.datasphere.runtime.components.CQSubTaskNoJoin;
import com.datasphere.runtime.containers.DynamicEventWrapper;
import com.datasphere.runtime.containers.IBatch;
import com.datasphere.runtime.containers.DARecord;
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

public class SelectGenerator extends Generator
{
    private final ExprGenerator exprGen;
    private final Compiler compiler;
    private final String cqName;
    private final String cqNamespace;
    private final Select select;
    private final TraceOptions traceOptions;
    private final DataSets dataSets;
    private final Map<String, ParamRef> parameters;
    private final Map<DataSet, List<IndexDesc>> indexes;
    private final List<SelectCompiler.Target> targets;
    private final Class<?> targetType;
    private final boolean targetIsObj;
    private final Map<String, Integer> targetFieldIndices;
    private final List<UUID> recordedTypes;
    private final List<FuncCall> aggregatedExprs;
    private final List<Condition.CondExpr> constantConditions;
    private final Map<DataSet, Condition> filterConditions;
    private final Map<DataSet, List<ValueExpr>> index0key;
    private final Map<DataSet, List<JoinDesc>> joinPlans;
    private final List<ValueExpr> groupBy;
    private final List<OrderByItem> orderBy;
    private final Predicate having;
    private final List<SelectCompiler.ModifyTarget> modifies;
    
    public SelectGenerator(final Compiler compiler, final String cqName, final String cqNamespace, final Select select, final TraceOptions traceOptions, final DataSets dataSets, final Map<String, ParamRef> parameters, final Map<DataSet, List<IndexDesc>> indexes, final List<SelectCompiler.Target> targets, final List<SelectCompiler.ModifyTarget> modifies, final Class<?> targetType, final boolean targetIsObj, final Map<String, Integer> targetFieldIndices, final List<UUID> recordedTypes, final List<FuncCall> aggregatedExprs, final List<Condition.CondExpr> constantConditions, final Map<DataSet, Condition> filterConditions, final Map<DataSet, List<ValueExpr>> index0key, final Map<DataSet, List<JoinDesc>> joinPlans, final List<ValueExpr> groupBy, final List<OrderByItem> orderBy, final Predicate having) {
        this.exprGen = new ExprGenerator(compiler, false) {
            @Override
            public void setPrevEventIndex(final int index) {
            }
        };
        this.compiler = compiler;
        this.cqName = cqName;
        this.cqNamespace = cqNamespace;
        this.select = select;
        this.traceOptions = traceOptions;
        this.dataSets = dataSets;
        this.parameters = parameters;
        this.indexes = indexes;
        this.targets = targets;
        this.modifies = modifies;
        this.targetType = targetType;
        this.targetIsObj = targetIsObj;
        this.targetFieldIndices = targetFieldIndices;
        this.recordedTypes = recordedTypes;
        this.aggregatedExprs = aggregatedExprs;
        this.constantConditions = constantConditions;
        this.filterConditions = filterConditions;
        this.index0key = index0key;
        this.joinPlans = joinPlans;
        this.groupBy = groupBy;
        this.orderBy = orderBy;
        this.having = having;
    }
    
    @Override
    public CQExecutionPlan generate() throws NotFoundException, CannotCompileException, IOException {
        final List<String> srcCodeList = new ArrayList<String>();
        final List<byte[]> subtasks = this.generateExecutionPlan(srcCodeList);
        final List<CQExecutionPlan.DataSource> sources = this.genDataSourcesInfo();
        final int indexCount = this.indexes.size();
        final List<Class<?>> aggFuncClasses = this.getAggFuncClasses();
        final List<RSFieldDesc> rsdesc = this.makeResultSetDesc();
        final List<ParamDesc> paramsDesc = this.makeParamsDesc();
        return new CQExecutionPlan(rsdesc, paramsDesc, this.select.kindOfStream, sources, subtasks, indexCount, aggFuncClasses, this.recordedTypes, this.haveGroupBy(), this.haveAggFuncs(), this.traceOptions, this.dataSets.size(), this.isQueryStateful(), false, srcCodeList);
    }
    
    private boolean haveGroupBy() {
        return !this.groupBy.isEmpty();
    }
    
    private boolean haveAggFuncs() {
        return !this.aggregatedExprs.isEmpty();
    }
    
    private boolean haveGroupByOrAggFuncs() {
        return this.haveGroupBy() || this.haveAggFuncs();
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
    
    private List<byte[]> generateExecutionPlan(final List<String> srcCodeList) throws NotFoundException, CannotCompileException, IOException {
        final String ns = this.cqNamespace;
        final WALoader wal = WALoader.get();
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
        final boolean haveJoin = this.dataSets.size() > 1;
        final Class<?> superClass = (Class<?>)(haveJoin ? CQSubTaskJoin.class : CQSubTaskNoJoin.class);
        final List<byte[]> codes = new ArrayList<byte[]>();
        for (final DataSet ds : this.dataSets) {
            if (ds.isIterator() == null) {
                final byte[] code = this.genSubTaskClass(wal, uri, ds, superClass, srcCodeList);
                codes.add(code);
            }
        }
        return codes;
    }
    
    private List<Class<?>> getAggFuncClasses() {
        final List<Class<?>> classes = new ArrayList<Class<?>>();
        for (final FuncCall fc : this.aggregatedExprs) {
            final Class<?> c = fc.getAggrClass();
            assert c != null;
            classes.add(c);
        }
        return classes;
    }
    
    private byte[] genSubTaskClass(final WALoader wal, final String bundleUri, final DataSet ds, final Class<?> superClass, final List<String> srcCodeList) throws NotFoundException, CannotCompileException, IOException {
        final List<String> methodsCode = this.genMethods(ds, superClass);
        final ClassPool pool = wal.getBundlePool(bundleUri);
        final StringBuilder src = new StringBuilder();
        final String className = ("QueryExecPlan_" + this.cqNamespace + "_" + this.cqName + "_" + ds.getFullName() + "_" + ds.getName()).replace('.', '_');
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
        byte[] code = (byte[])((dumpcode && debugPath) ? null : cc.toBytecode());
        final String sourceCode = src.toString();
        srcCodeList.add(sourceCode);
        if (dumpcode) {
            final PrintStream out = TraceOptions.getTraceStream(this.traceOptions);
            out.println(sourceCode);
            if (debugPath) {
                final String srcpath = this.traceOptions.traceFilePath + "/" + className + ".java";
                try (final FileOutputStream stream = new FileOutputStream(srcpath)) {
                    stream.write(sourceCode.getBytes());
                }
                code = CompilerUtils.compileJavaFile(srcpath);
            }
        }
        String report = null;
        try {
            report = CompilerUtils.verifyBytecode(code, wal);
        }
        catch (Exception e) {
            throw new IllegalArgumentException(e.getMessage() + " \n CODE: " + Arrays.toString(code) + " \n CODE TEXT " + sourceCode);
        }
        wal.addBundleClass(bundleUri, className, cc.toBytecode(), false);
        if (report != null) {
            throw new Error("Internal error: invalid bytecode\n" + report);
        }
        cc.detach();
        return code;
    }
    
    private List<String> genMethods(final DataSet ds, final Class<?> superClass) {
        this.exprGen.removeAllTmpVarsAndFrames();
        final List<String> methodsCode = new ArrayList<String>();
        methodsCode.add(this.genGetThisDS(ds, superClass));
        methodsCode.add(this.genUpdateIndexes(ds, superClass));
        methodsCode.add(this.genAddedFilter(ds, superClass));
        methodsCode.add(this.genRunImpl(ds, superClass));
        methodsCode.add(this.genRun(superClass));
        final List<JoinDesc> joins = this.joinPlans.get(ds);
        if (joins != null && !joins.isEmpty()) {
            final Iterator<JoinDesc> iterator = joins.iterator();
            if (iterator.hasNext()) {
                final JoinDesc join = iterator.next();
                methodsCode.add("public boolean isNotOuterJoin() { return " + !join.isOuter + "; } ");
            }
        }
        return methodsCode;
    }
    
    private String genUpdateIndexes(final DataSet ds, final Class<?> superClass) {
        final List<IndexDesc> idxs = this.indexes.get(ds);
        if (idxs == null) {
            return "public boolean isUpdateIndexesCodeGenerated()\n{\n return false;\n}\n";
        }
        final StringBuilder body = new StringBuilder();
        for (final IndexDesc idx : idxs) {
            final String key = "key" + idx.id;
            this.genKeyExprs(body, idx.exprs, idx.getIndexKeySignature(), key);
            body.append("this.context.updateIndex(" + idx.id + ", " + key + ", event, doadd);\n");
        }
        assert checkMethod("updateIndexes", superClass);
        return "public void updateIndexes(" + DARecord.class.getName() + " event, boolean doadd)\n{\n" + body.toString() + "return;\n}\n";
    }
    
    private String genGetThisDS(final DataSet ds, final Class<?> superClass) {
        assert checkMethod("getThisDS", superClass);
        return "public int getThisDS()\n{\nreturn " + ds.getID() + ";\n}\n";
    }
    
    private String genAddedFilter(final DataSet ds, final Class<?> superClass) {
        final List<ValueExpr> keyExprs = this.index0key.get(ds);
        if (keyExprs == null) {
            return null;
        }
        assert checkMethod("getAdded", superClass);
        assert checkMethod("getAddedFiltered", superClass);
        final String key = "filterkey" + ds.getID();
        final StringBuilder body = new StringBuilder();
        this.genKeyExprs(body, keyExprs, Expr.getSignature(keyExprs), key);
        return "public " + IBatch.class.getCanonicalName() + " getAdded()\n{\n" + body.toString() + "return getAddedFiltered(" + key + ");\n}\n";
    }
    
    private void genConstConditions(final StringBuilder body) {
        if (!this.constantConditions.isEmpty()) {
            body.append("//gen constant filter\n");
            for (final Condition.CondExpr cx : this.constantConditions) {
                final Pair<Var, String> p = this.genExpr(cx.expr);
                body.append(p.second);
                body.append("if(!" + p.first + ")\n\treturn;\n");
            }
        }
    }
    
    private void genFilterConditions(final DataSet ds, final StringBuilder body) {
        body.append("//gen filter\n");
        final Condition filter = this.filterConditions.get(ds);
        if (filter != null) {
            for (final Condition.CondExpr cx : filter.getCondExprs()) {
                final Pair<Var, String> p = this.genExpr(cx.expr);
                body.append(p.second);
                body.append("if(!" + p.first + ")\n\treturn;\n");
            }
        }
    }
    
    private void genJoins(final DataSet ds, final StringBuilder body, final Class<?> superClass) {
        body.append("//gen join\n");
        final List<JoinDesc> joins = this.joinPlans.get(ds);
        if (joins != null && !joins.isEmpty()) {
            body.append("// ---- join " + ds.getName() + " with ... \n");
            for (final JoinDesc join : joins) {
                final int withDS = join.withDataSet.getID();
                final String withDSName = join.withDataSet.getName();
                if (join.algorithm == JoinDesc.JoinAlgorithm.INDEX_LOOKUP) {
                    body.append("// ---- index join with " + withDSName + "\n");
                    final String key = "joinkey" + withDS;
                    this.genKeyExprs(body, join.searchExprs, join.getSearchKeySignature(), key);
                    assert checkMethod("createIndexIterator", superClass);
                    body.append("createIndexIterator(" + withDS + ", " + join.joinIndex.id + ", " + key + ");\n");
                }
                else if (join.algorithm == JoinDesc.JoinAlgorithm.WINDOW_INDEX_LOOKUP) {
                    body.append("// ---- window index join with " + withDSName + "\n");
                    final String key = "joinkey" + withDS;
                    this.genKeyExprs(body, join.searchExprs, join.getSearchKeySignature(), key);
                    assert checkMethod("createWindowLookupIterator", superClass);
                    body.append("createWindowLookupIterator(" + withDS + ", " + join.windowIndexID + ", " + key + ");\n");
                }
                else if (join.algorithm == JoinDesc.JoinAlgorithm.SCAN) {
                    body.append("// ---- cross join with " + withDSName + "\n");
                    final IteratorInfo it = join.withDataSet.isIterator();
                    if (it != null) {
                        final Pair<Var, String> fld = this.genExpr(it.getIterableField());
                        body.append(fld.second);
                        assert checkMethod("createCollectionIterator", superClass);
                        body.append("createCollectionIterator(" + withDS + ", " + fld.first + ");\n");
                    }
                    else {
                        assert checkMethod("createWindowIterator", superClass);
                        body.append("createWindowIterator(" + withDS + ");\n");
                    }
                }
                else if (join.algorithm == JoinDesc.JoinAlgorithm.WITH_STREAM) {
                    body.append("// ---- stream join with " + withDSName + "\n");
                    assert checkMethod("createEmptyIterator", superClass);
                    body.append("createEmptyIterator(" + withDS + ");\n");
                }
                else {
                    assert false;
                }
                assert checkMethod("fetch", superClass);
                if (join.isOuter) {
                    body.append("// ---- start outer join with " + withDSName + "\n");
                    body.append("boolean _found" + withDS + " = false;\n");
                    body.append("while(true) {\n");
                    body.append("    boolean _fetched" + withDS + " = fetch(" + withDS + ");\n");
                    body.append("    if(!_fetched" + withDS + ") {\n");
                    body.append("        if(_found" + withDS + ") break;\n");
                    body.append("        _found" + withDS + " = true;\n");
                    body.append("    }\n");
                    for (final Predicate pred : join.postFilters) {
                        final Pair<Var, String> p = this.genExpr(pred);
                        body.append("// after join condition\n");
                        body.append(p.second);
                        body.append("if(!" + p.first + ") continue;\n");
                    }
                    if (!join.afterJoinConditions.isEmpty()) {
                        body.append("// outer join condition\n");
                        for (final Condition c : join.afterJoinConditions) {
                            for (final Condition.CondExpr cx : c.getCondExprs()) {
                                final boolean isOuterJoinCond = cx.isOuterJoinCondtion(ds.id2bitset(), withDS);
                                if (isOuterJoinCond) {
                                    final Pair<Var, String> p2 = this.genExpr(cx.expr);
                                    body.append(p2.second);
                                    body.append("if(_fetched" + withDS + ") {\n");
                                    body.append("    if(!" + p2.first + ") continue;\n");
                                    body.append("}\n");
                                }
                            }
                        }
                    }
                    body.append("_found" + withDS + " = true;\n");
                    if (join.afterJoinConditions.isEmpty()) {
                        continue;
                    }
                    body.append("// after join condition\n");
                    for (final Condition c : join.afterJoinConditions) {
                        for (final Condition.CondExpr cx : c.getCondExprs()) {
                            final boolean isOuterJoinCond = cx.isOuterJoinCondtion(ds.id2bitset(), withDS);
                            if (!isOuterJoinCond) {
                                final Pair<Var, String> p2 = this.genExpr(cx.expr);
                                body.append(p2.second);
                                body.append("if(!" + p2.first + ") continue;\n");
                            }
                        }
                    }
                }
                else {
                    body.append("// ---- start join with " + join.withDataSet.getName() + "\n");
                    body.append("while(fetch(" + withDS + ")) {\n");
                    for (final Predicate pred : join.postFilters) {
                        final Pair<Var, String> p = this.genExpr(pred);
                        body.append(p.second);
                        body.append("if(!" + p.first + ") continue;\n");
                    }
                    if (join.afterJoinConditions.isEmpty()) {
                        continue;
                    }
                    for (final Condition c : join.afterJoinConditions) {
                        for (final Condition.CondExpr cx : c.getCondExprs()) {
                            final Pair<Var, String> p3 = this.genExpr(cx.expr);
                            body.append(p3.second);
                            body.append("if(!" + p3.first + ") continue;\n");
                        }
                    }
                }
            }
        }
        body.append("//most inner cycle\n");
    }
    
    private void genEndOfJoinLoop(final DataSet ds, final StringBuilder body) {
        final List<JoinDesc> joins = this.joinPlans.get(ds);
        if (joins != null && !joins.isEmpty()) {
            body.append("//end of join\n");
            for (final JoinDesc join : joins) {
                body.append("} // end of join with " + join.withDataSet.getName() + "\n");
            }
        }
    }
    
    private void genGroupByKey(final StringBuilder body, final Class<?> superClass) {
        body.append("//set group key\n");
        if (this.haveGroupByOrAggFuncs()) {
            assert checkMethod("setGroupByKeyAndAggVec", superClass);
            assert checkMethod("setGroupByKeyAndDefaultAggVec", superClass);
            if (this.haveGroupBy()) {
                final Class<?>[] groupByKeyType = Expr.getSignature(this.groupBy);
                this.genKeyExprs(body, this.groupBy, groupByKeyType, "groupkey");
                body.append("setGroupByKeyAndAggVec(groupkey);\n");
            }
            else {
                body.append("setGroupByKeyAndDefaultAggVec();\n");
            }
        }
    }
    
    private void genAggExpressions(final StringBuilder body) {
        body.append("//aggregate\n");
        if (this.haveAggFuncs()) {
            for (final FuncCall fc : this.aggregatedExprs) {
                body.append(this.exprGen.genAggExprs(fc));
            }
        }
    }
    
    private void genHaving(final StringBuilder body) {
        body.append("//having\n");
        if (this.having != null) {
            final Pair<Var, String> p = this.genExpr(this.having);
            body.append(p.second);
            body.append("if(" + p.first + ") {\n");
        }
    }
    
    private void genModifyEvent(final StringBuilder body) {
        final List<ModifyExpr> modifyExprs = this.select.modify;
        if (modifyExprs == null) {
            return;
        }
        if (this.modifies.isEmpty()) {
            return;
        }
        body.append("res.data = com.datasphere.runtime.BuiltInFunc.CLONE(res.data); \n");
        body.append("//gen modify clause\n");
        for (final SelectCompiler.ModifyTarget modifyTarget : this.modifies) {
            final Pair<Var, String> cc = this.genExpr(modifyTarget.expr);
            body.append(cc.second + "\n");
            body.append("res.data[" + modifyTarget.index + "] = com.datasphere.runtime.BuiltInFunc.TO_OBJ(" + cc.first.toString() + ");\n");
        }
        body.append("//end of modify clause\n");
    }
    
    private void genOutputEvent(final StringBuilder body) {
        body.append("//output\n");
        if (this.targetType != null) {
            String targetTypeName = this.targetType.getName();
            if (!this.targetIsObj) {
                body.append(targetTypeName + " res = new " + targetTypeName + "(System.currentTimeMillis());\n");
                body.append("boolean setFieldFlag = (res.fieldIsSet != null);\n");
                body.append("res.allFieldsSet = " + ((this.targets.size() == this.targetFieldIndices.size()) ? "true" : "false") + ";\n");
                if (this.select.distinct) {
                    body.append("Object[] tmp = new Object[" + this.targets.size() + "];\n");
                }
                int i = 0;
                for (final SelectCompiler.Target t : this.targets) {
                    final Pair<Var, String> s = this.genExpr(t.expr);
                    body.append(s.second);
                    if (this.select.distinct) {
                        body.append("tmp[" + i + "] = " + FieldToObject.genConvert(s.first.getName()) + ";\n");
                    }
                    ++i;
                    body.append("res." + t.targetField.getName() + "=" + s.first + ";\n");
                    final Integer index = this.targetFieldIndices.get(t.targetField.getName());
                    if (index != null) {
                        final int pos = index / 7;
                        final int offset = index % 7;
                        final byte bitset = (byte)(1 << offset);
                        body.append("if (setFieldFlag) res.fieldIsSet[" + pos + "] |= " + bitset + ";\n");
                    }
                }
                if (this.select.distinct) {
                    assert checkMethod("setPayload", QueryResultEvent.class);
                    body.append("res.payload = tmp;\n");
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
            for (final SelectCompiler.Target t : this.targets) {
                final Pair<Var, String> s = this.genExpr(t.expr);
                body.append(s.second);
                body.append("tmp[" + i + "] = " + FieldToObject.genConvert(s.first.getName()) + ";\n");
                ++i;
            }
            body.append(targetTypeName + " res = new " + targetTypeName + "(System.currentTimeMillis());\n");
            assert checkMethod("setPayload", QueryResultEvent.class);
            body.append("res.setPayload(tmp);\n");
        }
        body.append("setOutputEvent(res);\n");
    }
    
    private void genOrderByKey(final StringBuilder body) {
        body.append("//order by key\n");
        if (!this.orderBy.isEmpty()) {
            final List<ValueExpr> orderByExprs = this.getOrderByExprs();
            final Class<?>[] orderByKeyType = Expr.getSignature(orderByExprs);
            this.genKeyExprs(body, orderByExprs, orderByKeyType, "orderkey", true);
            body.append("setOrderByKey(orderkey);\n");
        }
    }
    
    private void addOutputEventToBatch(final StringBuilder body, final Class<?> superClass) {
        assert checkMethod("addEvent", superClass);
        if (this.select.linksrc) {
            body.append("linkSourceEvents(); // link source events\n");
        }
        body.append("addEvent(); //add result to the batch\n");
    }
    
    private void genEndOfHaving(final StringBuilder body, final Class<?> superClass) {
        assert checkMethod("removeGroupKey", superClass);
        if (this.having != null) {
            if (this.haveGroupByOrAggFuncs()) {
                body.append("} else { removeGroupKey(); } // end of having\n");
            }
            else {
                body.append("} // end of having\n");
            }
        }
    }
    
    private String genRunImpl(final DataSet ds, final Class<?> superClass) {
        assert ds.isIterator() == null;
        final StringBuilder body = new StringBuilder();
        this.genConstConditions(body);
        this.genFilterConditions(ds, body);
        this.genJoins(ds, body, superClass);
        this.genGroupByKey(body, superClass);
        this.genAggExpressions(body);
        this.genHaving(body);
        this.genOutputEvent(body);
        this.genModifyEvent(body);
        this.genOrderByKey(body);
        this.addOutputEventToBatch(body, superClass);
        this.genEndOfHaving(body, superClass);
        this.genEndOfJoinLoop(ds, body);
        assert checkMethod("runImpl", superClass);
        return "public void runImpl()\n{\n" + body.toString() + "return;\n}\n";
    }
    
    private String genRun(final Class<?> superClass) {
        assert checkMethod("initResultBatchBuilder", superClass);
        assert checkMethod("run", superClass);
        final StringBuilder body = new StringBuilder();
        body.append("public void run()\n{\n\t");
        body.append("initResultBatchBuilder(\n\t");
        if (this.haveGroupByOrAggFuncs()) {
            body.append("makeGrouped()");
        }
        else {
            body.append("makeNotGrouped()");
        }
        body.append(",\n\t");
        final List<OrderByItem> ord = this.orderBy.isEmpty() ? null : this.select.orderBy;
        if (ord != null) {
            body.append("makeOrdered(" + ord.get(0).isAscending + ")");
        }
        else {
            body.append("makeNotOrdered()");
        }
        body.append(",\n\t");
        if (this.select.distinct) {
            body.append("makeRemoveDups()");
        }
        else {
            body.append("makeNotRemoveDups()");
        }
        body.append(",\n\t");
        final LimitClause lim = this.select.limit;
        if (lim != null) {
            body.append("makeLimited(" + lim.offset + ", " + lim.limit + ")");
        }
        else {
            body.append("makeNotLimited()");
        }
        body.append(");\n\t");
        if (this.haveAggFuncs()) {
            body.append("processAggregated()");
        }
        else {
            body.append("processNotAggregated()");
        }
        body.append(";\n}\n");
        return body.toString();
    }
    
    private List<ValueExpr> getOrderByExprs() {
        return new AbstractList<ValueExpr>() {
            @Override
            public ValueExpr get(final int index) {
                return SelectGenerator.this.orderBy.get(index).expr;
            }
            
            @Override
            public int size() {
                return SelectGenerator.this.orderBy.size();
            }
        };
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
    
    private void genKeyExprs(final StringBuilder body, final List<? extends Expr> exprs, final Class<?>[] sig, final String keyVar) {
        this.genKeyExprs(body, exprs, sig, keyVar, false);
    }
    
    private void genKeyExprs(final StringBuilder body, final List<? extends Expr> exprs, final Class<?>[] sig, final String keyVar, final boolean unique) {
        final Pair<List<Var>, String> keyCode = this.genExprs(exprs);
        body.append(keyCode.second);
        final List<String> keyArgs = new ArrayList<String>();
        for (final Var v : keyCode.first) {
            keyArgs.add(v.getName());
        }
        assert sig.length == keyArgs.size();
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
        if (unique) {
            keyArgs.add("genNextInt()");
        }
        final String args = StringUtils.join((List)keyArgs);
        final String recKeyType = RecordKey.class.getName();
        final String factory = RecordKey.getObjArrayKeyFactory();
        final String argArray = keyVar + "Args";
        body.append("Object[] " + argArray + " = { " + args + " };\n");
        body.append(recKeyType + " " + keyVar + " =\n\t" + factory + "(" + argArray + ");\n");
    }
    
    private void makeMethod(final String mCode, final CtClass cc, final StringBuilder src) {
        try {
            final CtMethod m = CtNewMethod.make(mCode, cc);
            cc.addMethod(m);
            src.append("\n").append(mCode);
        }
        catch (CannotCompileException e) {
            throw new RuntimeException("error in generated Java code\n" + mCode, (Throwable)e);
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
