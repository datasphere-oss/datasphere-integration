package com.datasphere.runtime.compiler.select;

import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.fasterxml.jackson.databind.JsonNode;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.metaRepository.MetadataRepository;
import com.datasphere.proc.events.DynamicEvent;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.Pair;
import com.datasphere.runtime.TraceOptions;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.CompilerUtils;
import com.datasphere.runtime.compiler.TypeName;
import com.datasphere.runtime.compiler.exprs.CastExprFactory;
import com.datasphere.runtime.compiler.exprs.Constant;
import com.datasphere.runtime.compiler.exprs.DataSetRef;
import com.datasphere.runtime.compiler.exprs.Expr;
import com.datasphere.runtime.compiler.exprs.ExprCmd;
import com.datasphere.runtime.compiler.exprs.FieldRef;
import com.datasphere.runtime.compiler.exprs.FuncCall;
import com.datasphere.runtime.compiler.exprs.IndexExpr;
import com.datasphere.runtime.compiler.exprs.LogicalPredicate;
import com.datasphere.runtime.compiler.exprs.ModifyExpr;
import com.datasphere.runtime.compiler.exprs.ObjectRef;
import com.datasphere.runtime.compiler.exprs.ParamRef;
import com.datasphere.runtime.compiler.exprs.Predicate;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.compiler.exprs.WildCardExpr;
import com.datasphere.runtime.compiler.patternmatch.MatchGenerator;
import com.datasphere.runtime.compiler.patternmatch.MatchValidator;
import com.datasphere.runtime.compiler.stmts.OrderByItem;
import com.datasphere.runtime.compiler.stmts.Select;
import com.datasphere.runtime.compiler.stmts.SelectTarget;
import com.datasphere.runtime.compiler.visitors.ConditionAnalyzer;
import com.datasphere.runtime.compiler.visitors.ExprValidationVisitor;
import com.datasphere.runtime.compiler.visitors.FindCommonSubexpression;
import com.datasphere.runtime.containers.DynamicEventWrapper;
import com.datasphere.runtime.containers.HDEvent;
import com.datasphere.runtime.exceptions.AmbiguousFieldNameException;
import com.datasphere.runtime.meta.CQExecutionPlan;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.Factory;
import com.datasphere.runtime.utils.NamePolicy;
import com.datasphere.security.HSecurityManager;
import com.datasphere.uuid.UUID;
import com.datasphere.hdstore.HDStoreManager;
import com.datasphere.hdstore.HDStores;

public class SelectCompiler
{
    private DataSets dataSets;
    private int nextDataSetID;
    private int nextIndexID;
    private final List<Condition.CondExpr> constantConditions;
    private final Map<Condition.CondKey, Condition> joinConditions;
    private final Map<Condition.CondKey, Condition> afterJoinFilterConditions;
    private final Map<DataSet, Condition> filterConditions;
    private final Map<DataSet, List<ValueExpr>> index0key;
    private final Map<DataSet, List<JoinDesc>> joinPlans;
    private final Map<DataSet, List<IndexDesc>> indexes;
    private final Map<Expr, Expr> exprIndex;
    private final List<Predicate> predicates;
    private final List<ValueExpr> groupBy;
    private final List<OrderByItem> orderBy;
    private final List<FuncCall> aggregatedExprs;
    private final List<Target> targets;
    private final List<ModifyTarget> modifies;
    private Predicate having;
    private boolean haveAggFuncs;
    private boolean targetIsObj;
    private final Map<Integer, Expr> eliminated;
    private final List<UUID> recordedTypes;
    private final Map<String, Expr> alias2expr;
    private Join.Tree joinTree;
    private final List<Condition.CondExpr> condIndex;
    private final Map<String, ParamRef> parameters;
    private final String cqName;
    private final String cqNamespace;
    private final Compiler compiler;
    private final Select select;
    private final Class<?> targetType;
    private final List<Field> targetFields;
    private final Map<String, Integer> targetFieldIndices;
    private final TraceOptions traceOptions;
    private final boolean isAdhoc;
    private MatchValidator matchValidator;
    private UUID outputTypeId;
    
    public static CQExecutionPlan compileSelect(final String cqName, final String namespaceName, final Compiler compiler, final Select select, final TraceOptions traceOptions, final boolean isAdhoc) throws Exception {
        return compileSelectInto(cqName, namespaceName, compiler, select, traceOptions, null, null, null, null, isAdhoc);
    }
    
    public static CQExecutionPlan compileSelect(final String cqName, final String namespaceName, final Compiler compiler, final Select select, final TraceOptions traceOptions) throws Exception {
        return compileSelectInto(cqName, namespaceName, compiler, select, traceOptions, null, null, null, null, false);
    }
    
    public static CQExecutionPlan compileSelectInto(final String cqName, final String namespaceName, final Compiler compiler, final Select select, final TraceOptions traceOptions, final Class<?> targetType, final List<Field> targetFields, final Map<String, Integer> targetFieldIndices, final UUID outputTypeId, final boolean isAdhoc) throws Exception {
        final SelectCompiler c = new SelectCompiler(cqName, namespaceName, compiler, select, targetType, targetFields, targetFieldIndices, traceOptions, isAdhoc);
        if (outputTypeId != null) {
            c.addTypeID(outputTypeId);
        }
        c.addOutputTypeId(outputTypeId);
        return c.compile();
    }
    
    private SelectCompiler(final String cqName, final String namespaceName, final Compiler compiler, final Select select, final Class<?> targetType, final List<Field> targetFields, final Map<String, Integer> targetFieldIndices, final TraceOptions traceOptions, final boolean isAdhoc) {
        this.dataSets = new DataSets();
        this.nextDataSetID = 0;
        this.nextIndexID = 0;
        this.constantConditions = new ArrayList<Condition.CondExpr>();
        this.joinConditions = Factory.makeMap();
        this.afterJoinFilterConditions = Factory.makeMap();
        this.filterConditions = Factory.makeMap();
        this.index0key = Factory.makeMap();
        this.joinPlans = Factory.makeMap();
        this.indexes = Factory.makeMap();
        this.exprIndex = Factory.makeLinkedMap();
        this.predicates = new ArrayList<Predicate>();
        this.groupBy = new ArrayList<ValueExpr>();
        this.orderBy = new ArrayList<OrderByItem>();
        this.aggregatedExprs = new ArrayList<FuncCall>();
        this.targets = new ArrayList<Target>();
        this.modifies = new ArrayList<ModifyTarget>();
        this.having = null;
        this.haveAggFuncs = false;
        this.targetIsObj = false;
        this.eliminated = Factory.makeLinkedMap();
        this.recordedTypes = new ArrayList<UUID>();
        this.alias2expr = Factory.makeMap();
        this.joinTree = null;
        this.condIndex = new ArrayList<Condition.CondExpr>();
        this.parameters = Factory.makeNameLinkedMap();
        this.outputTypeId = UUID.nilUUID();
        this.cqName = cqName;
        this.cqNamespace = namespaceName;
        this.compiler = compiler;
        this.targetType = targetType;
        this.targetFields = targetFields;
        this.targetFieldIndices = targetFieldIndices;
        this.select = select;
        this.isAdhoc = isAdhoc;
        this.traceOptions = traceOptions;
    }
    
    private CQExecutionPlan compile() throws Exception {
        this.validate();
        if (this.compiler.getContext().isReadOnly()) {
            return null;
        }
        this.analyze();
        Generator gen;
        if (!this.haveMatch()) {
            gen = new SelectGenerator(this.compiler, this.cqName, this.cqNamespace, this.select, this.traceOptions, this.dataSets, this.parameters, this.indexes, this.targets, this.modifies, this.targetType, this.targetIsObj, this.targetFieldIndices, this.recordedTypes, this.aggregatedExprs, this.constantConditions, this.filterConditions, this.index0key, this.joinPlans, this.groupBy, this.orderBy, this.having);
        }
        else {
            gen = new MatchGenerator(this.compiler, this.cqName, this.cqNamespace, this.select, this.traceOptions, this.dataSets, this.parameters, this.matchValidator.getTargets(), this.targetType, this.targetIsObj, this.targetFieldIndices, this.recordedTypes, this.matchValidator.getVariables(), this.matchValidator.getPartitionKey(), this.matchValidator.getPattern());
        }
        final CQExecutionPlan plan = gen.generate();
        if ((this.traceOptions.traceFlags & 0x8) > 0) {
            final PrintStream out = TraceOptions.getTraceStream(this.traceOptions);
            this.dump(out);
        }
        return plan;
    }
    
    private void error(final String message, final Object info) {
        this.compiler.error(message, info);
    }
    
    private void addTypeID(final UUID type) {
        this.recordedTypes.add(type);
    }
    
    private void addOutputTypeId(final UUID outputTypeId) {
        if (outputTypeId != null) {
            this.outputTypeId = outputTypeId;
        }
    }
    
    private ValueExpr cast(final ValueExpr e, final Class<?> targetType) {
        return CastExprFactory.cast(this.compiler, e, targetType);
    }
    
    private DataSet getDataSet(final int id) {
        return this.dataSets.get(id);
    }
    
    private boolean haveGroupBy() {
        return !this.groupBy.isEmpty();
    }
    
    private boolean haveMatch() {
        return this.select.match != null;
    }
    
    private void validate() throws MetaDataRepositoryException {
        this.validateFrom();
        this.validateMatch();
        this.validateWhere();
        this.validateTargets();
        this.validateModify();
        this.validateGroupBy();
        this.validateHaving();
        this.validateTargetStreamFields();
        this.validateOrderBy();
    }
    
    private void analyze() throws MetaDataRepositoryException {
        this.analyzeMatch();
        this.analyzePredicates();
        this.buildJoinPlan();
        this.analyzeGropyBy();
        this.analyzeHavingAndTargets();
        this.analyzeOrderBy();
        this.analyzeIndexedCondtions();
        this.pushDownFilters();
        this.analyzeWAStoreQuery();
    }
    
    private void analyzeMatch() {
        if (this.haveMatch()) {
            this.matchValidator.analyze();
        }
    }
    
    private void attachJsonQueryToWASView(final DataSet ds, final JsonNode query) throws MetaDataRepositoryException {
        final MetaInfo.WAStoreView w = ds.isJsonWAStore().getStoreView();
        final byte[] wasquery = AST2JSON.serialize(query);
        w.setQuery(wasquery);
        final Context ctx = this.compiler.getContext();
        ctx.updateWAStoreView(w);
    }
    
    private void pushDownFilters() throws MetaDataRepositoryException {
        for (final DataSet ds : this.dataSets) {
            final JsonWAStoreInfo eds = ds.isJsonWAStore();
            if (eds != null) {
                final Condition filter = this.filterConditions.get(ds);
                final HDStoreManager m = HDStores.getInstance(eds.getStore().properties);
                final JsonNode query = new AST2JSON(m.getCapabilities()).transformFilter(ds, filter);
                this.attachJsonQueryToWASView(ds, query);
            }
        }
    }
    
    private void analyzeWAStoreQuery() throws MetaDataRepositoryException {
        if (this.haveGroupBy() && !this.haveAggFuncs) {
            return;
        }
        if (this.dataSets.size() != 1) {
            return;
        }
        final DataSet ds = this.dataSets.get(0);
        final JsonWAStoreInfo eds = ds.isJsonWAStore();
        if (eds == null) {
            return;
        }
        final MetaInfo.WAStoreView w = eds.getStoreView();
        if (w.subscribeToUpdates) {
            return;
        }
        final HDStoreManager m = HDStores.getInstance(eds.getStore().properties);
        final JsonNode query = new AST2JSON(m.getCapabilities()).transform(this.dataSets, this.predicates, this.targets, this.groupBy, this.having, this.orderBy);
        if (query == null) {
            return;
        }
        this.attachJsonQueryToWASView(ds, query);
        this.predicates.clear();
        this.groupBy.clear();
        this.orderBy.clear();
        this.aggregatedExprs.clear();
        this.having = null;
        this.haveAggFuncs = false;
        final List<RSFieldDesc> fields = new ArrayList<RSFieldDesc>();
        for (final Target t : this.targets) {
            String name = null;
            final Expr e = AST2JSON.skipCastOps(t.expr);
            if (e instanceof FieldRef) {
                name = ((FieldRef)e).getName();
            }
            else if (e instanceof FuncCall) {
                final FuncCall c = (FuncCall)e;
                final String funcName = c.getName().toLowerCase();
                if (c.args.size() == 0) {
                    name = "$id:" + funcName;
                }
                else {
                    final Expr e2 = AST2JSON.skipCastOps(c.args.get(0));
                    name = ((FieldRef)e2).getName() + ":" + funcName;
                }
            }
            else {
                assert false;
                throw new RuntimeException("internla error: cannot transform elasticsearch query");
            }
            fields.add(new RSFieldDesc(name, t.expr.getType()));
        }
        final DataSet newds = DataSetFactory.createJSONWAStoreDS(ds.getID(), ds.getName(), null, fields, eds.getStoreView(), eds.getStore());
        final List<FieldRef> flds = newds.makeListOfAllFields();
        assert this.targets.size() == flds.size();
        int i = 0;
        for (final Target t2 : this.targets) {
            t2.expr = flds.get(i++);
        }
        (this.dataSets = new DataSets()).add(newds);
    }
    
    private void dump(final PrintStream out) {
        this.dumpDataSets(out);
        this.dumpConditions(out);
        this.dumpIndexes(out);
        this.dumpJoinPlanEveryDataSet(out);
    }
    
    private DataSource.Resolver makeResolver() {
        return new DataSource.Resolver() {
            @Override
            public Class<?> getTypeInfo(final TypeName typeName) {
                return SelectCompiler.this.compiler.getClass(typeName);
            }
            
            @Override
            public TraceOptions getTraceOptions() {
                return SelectCompiler.this.traceOptions;
            }
            
            @Override
            public int getNextDataSetID() {
                return SelectCompiler.this.nextDataSetID++;
            }
            
            @Override
            public Compiler getCompiler() {
                return SelectCompiler.this.compiler;
            }
            
            @Override
            public void error(final String message, final Object info) {
                SelectCompiler.this.compiler.error(message, info);
            }
            
            @Override
            public void addTypeID(final UUID dataType) {
                SelectCompiler.this.recordedTypes.add(dataType);
            }
            
            @Override
            public void addPredicate(final Predicate p) {
                SelectCompiler.this.predicates.add(p);
            }
            
            @Override
            public Join.Node addDataSourceJoin(final DataSourceJoin dsJoin) {
                if (dsJoin.joinCondition == null && dsJoin.kindOfJoin.isOuter()) {
                    this.error("outer join requires join condition", dsJoin);
                }
                final DataSets tmpdatasets = SelectCompiler.this.dataSets;
                SelectCompiler.this.dataSets = new DataSets();
                final Join.Node nleft = dsJoin.left.addDataSource(this);
                final Join.Node nright = dsJoin.right.addDataSource(this);
                Predicate jcond;
                if (dsJoin.joinCondition != null) {
                    jcond = SelectCompiler.this.validatePredicate(dsJoin.joinCondition, false);
                }
                else {
                    jcond = null;
                }
                for (final DataSet ds : SelectCompiler.this.dataSets) {
                    if (!tmpdatasets.add(ds)) {
                        this.error("datasource name duplication", ds.getName());
                    }
                }
                SelectCompiler.this.dataSets = tmpdatasets;
                return Join.createJoinNode(nleft, nright, jcond, dsJoin.kindOfJoin);
            }
            
            @Override
            public Join.Node addDataSet(final DataSet ds) {
                if (!SelectCompiler.this.dataSets.add(ds)) {
                    this.error("datasource name duplication", ds.getName());
                }
                return Join.createDataSetNode(ds);
            }
            
            @Override
            public boolean isAdhoc() {
                return SelectCompiler.this.isAdhoc;
            }
        };
    }
    
    private void validateFrom() throws MetaDataRepositoryException {
        final DataSource.Resolver r = this.makeResolver();
        final Map<String, DataSet> resolved = NamePolicy.makeNameMap();
        List<DataSourcePrimary> unresolved = new ArrayList<DataSourcePrimary>();
        for (final DataSource ds : this.select.from) {
            ds.resolveDataSource(r, resolved, unresolved, true);
        }
        while (!unresolved.isEmpty()) {
            final List<DataSourcePrimary> leftunresolved = new ArrayList<DataSourcePrimary>();
            for (final DataSourcePrimary pds : unresolved) {
                pds.resolveDataSource(r, resolved, leftunresolved, false);
            }
            if (leftunresolved.size() == unresolved.size()) {
                this.error("cannot resolved iterator", unresolved.get(0));
            }
            unresolved = leftunresolved;
        }
        Join.Node root = null;
        for (final DataSource ds2 : this.select.from) {
            final Join.Node n = ds2.addDataSource(r);
            if (root == null) {
                root = n;
            }
            else {
                root = Join.createJoinNode(root, n, null, Join.Kind.CROSS);
            }
        }
        this.joinTree = Join.createJoinTree(root);
    }
    
    private void validateMatch() {
        if (this.haveMatch()) {
            (this.matchValidator = new MatchValidator(this.compiler, this.parameters, this.dataSets)).validate(this.select.match, this.select.from, this.select.targets);
        }
    }
    
    private void validateWhere() {
        if (this.select.where != null) {
            this.validatePredicate(this.select.where, false);
        }
    }
    
    private ValueExpr checkExprIsIntConst(final ValueExpr e) {
        if (e.op == ExprCmd.INT) {
            final Constant c = (Constant)e;
            final int index = (int)c.value;
            if (index >= 0 && index <= this.select.targets.size()) {
                final SelectTarget t = this.select.targets.get(index - 1);
                if (t.expr != null) {
                    return t.expr;
                }
                this.error("cannot refer expression with index " + index, e);
            }
            this.error("cannot find expression with index " + index, e);
        }
        return e;
    }
    
    private void validateGroupBy() {
        if (this.select.groupBy != null) {
            for (final ValueExpr e : this.select.groupBy) {
                final ValueExpr ee = this.checkExprIsIntConst(e);
                final ValueExpr enew = this.validateValueExpr(ee, false);
                this.checkComparable(enew);
                this.groupBy.add(enew);
            }
        }
    }
    
    private void validateOrderBy() {
        if (this.select.orderBy != null) {
            for (final OrderByItem e : this.select.orderBy) {
                final ValueExpr ee = this.checkExprIsIntConst(e.expr);
                final ValueExpr enew = this.validateValueExpr(ee, true);
                this.checkComparable(enew);
                this.orderBy.add(new OrderByItem(enew, e.isAscending));
            }
        }
    }
    
    private void validateHaving() {
        if (this.select.having != null) {
            this.having = this.validatePredicate(this.select.having, true);
        }
    }
    
    private void validateTargets() {
        if (this.haveMatch()) {
            return;
        }
        for (final SelectTarget t : this.select.targets) {
            if (t.expr != null) {
                final ValueExpr e = this.validateValueExpr(t.expr, true, true);
                if (e instanceof WildCardExpr) {
                    final WildCardExpr wc = (WildCardExpr)e;
                    final Iterator<String> aliases = wc.getAliases().iterator();
                    for (final ValueExpr ve : wc.getExprs()) {
                        final String alias = aliases.next();
                        final Target newt = new Target(ve, alias);
                        this.targets.add(newt);
                        this.alias2expr.put(alias, ve);
                    }
                }
                else {
                    final Target newt2 = new Target(e, t.alias);
                    this.targets.add(newt2);
                    this.alias2expr.put(t.alias, e);
                }
            }
            else {
                final List<FieldRef> allFields = this.makeListOfAllFields();
                for (final FieldRef r : allFields) {
                    final String alias2 = r.getName();
                    final Target newt3 = new Target(r, alias2);
                    this.targets.add(newt3);
                }
            }
        }
    }
    
    private void validateModifyGeneratedType() {
        try {
            final MetaInfo.MetaObject outputTypeMetaObject = MetadataRepository.getINSTANCE().getMetaObjectByUUID(this.outputTypeId, HSecurityManager.TOKEN);
            if (outputTypeMetaObject == null || !(outputTypeMetaObject instanceof MetaInfo.Type) || !((MetaInfo.Type)outputTypeMetaObject).generated) {
                this.error("Output stream do not use generated type and only HDEvent is supported as generated type", this.select.from);
            }
        }
        catch (MetaDataRepositoryException e2) {
            this.error("Modify clause will not work except two cases: HDEvent and Generated Type streams. Please use one of the two types.", this.select);
        }
        if (this.select.modify != null) {
            final SelectTarget output = this.select.targets.get(0);
            if (output.expr != null) {
                this.error("Expecting only SELECT * for the generated types ", this.select);
            }
            for (final ModifyExpr modifyExpr : this.select.modify) {
                if (modifyExpr.args.size() < 2) {
                    this.error("Expected two side for modify clause", modifyExpr);
                }
                if (!(modifyExpr.args.get(0) instanceof ValueExpr)) {
                    this.error("Expected field reference on the left side of the modify clause", modifyExpr);
                }
                final ValueExpr valueExpr = (ValueExpr)modifyExpr.args.get(0);
                if (!(valueExpr instanceof ObjectRef)) {
                    this.error("Left expression do not refer to an object", modifyExpr);
                }
                final Integer index = this.targetFieldIndices.get( (IndexExpr)modifyExpr.args.get(0).name);
                if (index == null) {
                    this.error("Could not find field that modify clause tries to access fields are \n: " + this.targetFieldIndices.entrySet() + " \n", modifyExpr);
                }
                final SelectTarget rSide = (SelectTarget)modifyExpr.args.get(1);
                if (rSide == null) {
                    this.error("Right side expression did not form correctly", modifyExpr);
                }
                final ValueExpr e = this.validateValueExpr(rSide.expr, true, true);
                if (e == null) {
                    this.error("Cannot validate the expression " + rSide.expr, modifyExpr);
                }
                final Target target = this.targets.get(index);
                target.expr = e;
            }
        }
    }
    
    private void validateModify() {
        if (this.select.modify == null) {
            return;
        }
        final int targetSize = this.select.targets.size();
        if (targetSize != 1) {
            this.error("only one projection field is allowed", this.select.targets);
        }
        if (this.targetType.equals(HDEvent.class)) {
            this.validateModifyNonGeneratedType();
        }
        else {
            this.validateModifyGeneratedType();
        }
        final SelectTarget selectTarget = this.select.targets.get(0);
        this.postCheckForModifyValidation(selectTarget);
    }
    
    void postCheckForModifyValidation(final SelectTarget selectTarget) {
        final Target firstField = this.targets.get(0);
        if (!(firstField.expr instanceof DataSetRef) && selectTarget.expr != null) {
            this.error("cannot use wildcard expr with modify", this.select.targets);
        }
    }
    
    private void validateModifyNonGeneratedType() {
        if (this.haveMatch()) {
            return;
        }
        if (this.select.modify != null) {
            Integer index = null;
            for (final ModifyExpr modifyExpr : this.select.modify) {
                if (modifyExpr.args.size() < 2) {
                    this.error("expression is not correctly formed", this.select.modify);
                }
                if (modifyExpr.args.get(0) instanceof IndexExpr) {
                    final IndexExpr indexExpr = (IndexExpr)modifyExpr.args.get(0);
                    Constant constant = null;
                    if (indexExpr.args.size() != 2) {
                        this.error("index expression is not correctly formed", this.select.modify);
                    }
                    if (indexExpr.args.get(1) instanceof Constant) {
                        constant = indexExpr.args.get(1);
                    }
                    else {
                        this.error("left expression not a constant expr", this.select.modify);
                    }
                    if (constant.value instanceof Integer) {
                        index = (Integer)constant.value;
                    }
                    else {
                        this.error("index expression not integer", this.select.modify);
                    }
                }
                else {
                    this.error("left expression needs to be Index expression", this.select.modify);
                }
                final ValueExpr e = modifyExpr.args.get(1).expr;
                if (e instanceof ParamRef || e instanceof WildCardExpr) {
                    this.error("Cannot be parametrized expr or wildcard expr", this.select.modify);
                }
                final ValueExpr validateValueExpr = this.validateValueExpr(e, true, true);
                this.modifies.add(new ModifyTarget(index, validateValueExpr));
            }
        }
    }
    
    private ValueExpr validateTargetType(final ValueExpr e, final Field target) {
        assert e != null;
        final Class<?> targetType = target.getType();
        final Class<?> sourceType = e.getType();
        if (!CompilerUtils.isCastable(targetType, sourceType)) {
            final Object o = (this.compiler.getExprText(e) == null) ? null : e;
            this.error("incompatible types of field <" + target.getName() + "(" + targetType.getSimpleName() + ")> and SELECT expression (" + sourceType.getSimpleName() + ")", o);
        }
        return this.cast(e, targetType);
    }
    
    private void validateTargetStreamFields() {
        final List<Target> selTargets = this.haveMatch() ? this.matchValidator.getTargets() : this.targets;
        final List<SelectTarget> selectTargetsSrc = this.select.targets;
        if (this.targetFields == null) {
            return;
        }
        if (this.targetFields.isEmpty()) {
            if (selTargets.size() == 1) {
                final ValueExpr e = selTargets.get(0).expr;
                assert e != null;
                Class<?> etype = e.getType();
                if (etype.equals(DynamicEventWrapper.class)) {
                    etype = DynamicEvent.class;
                }
                if (this.targetType.isAssignableFrom(etype)) {
                    this.targetIsObj = true;
                    return;
                }
            }
            final List<Field> nsfields = CompilerUtils.getNonStaticFields(this.targetType);
            final List<Field> fields = CompilerUtils.getNotSpecial(nsfields);
            final int fieldsSize = fields.size();
            final int targetsSize = selTargets.size();
            if (fieldsSize != targetsSize) {
                this.error("the number of SELECT expressions (" + targetsSize + ") not equal to the number of fields in output stream (" + fieldsSize + ")", selectTargetsSrc);
            }
            for (int i = 0; i < selTargets.size(); ++i) {
                final Target t = selTargets.get(i);
                t.targetField = fields.get(i);
                t.expr = this.validateTargetType(t.expr, t.targetField);
            }
        }
        else {
            final int fieldsSize2 = this.targetFields.size();
            final int targetsSize2 = selTargets.size();
            if (fieldsSize2 != targetsSize2) {
                this.error("different number of output fields (" + fieldsSize2 + ") and SELECT expressions (" + targetsSize2 + ")", selectTargetsSrc);
            }
            for (int j = 0; j < selTargets.size(); ++j) {
                final Target t2 = selTargets.get(j);
                t2.targetField = this.targetFields.get(j);
                t2.expr = this.validateTargetType(t2.expr, t2.targetField);
            }
        }
    }
    
    private ValueExpr validateValueExpr(final ValueExpr e, final boolean canHaveAggFunc) {
        return this.validateValueExpr(e, canHaveAggFunc, false);
    }
    
    private Expr validateExpr(final Expr e, final boolean canHaveAggFunc, final boolean acceptWildcard) {
        final ExprValidator ctx = new ExprValidator(this.compiler, this.parameters, canHaveAggFunc, acceptWildcard) {
            @Override
            public void setHaveAggFuncs() {
                SelectCompiler.this.haveAggFuncs = true;
            }
            
            @Override
            public DataSet getDataSet(final String name) {
                return SelectCompiler.this.dataSets.get(name);
            }
            
            @Override
            public FieldRef findField(final String name) {
                try {
                    return SelectCompiler.this.dataSets.findField(name);
                }
                catch (AmbiguousFieldNameException e) {
                    this.error("ambigious field name reference", name);
                    return null;
                }
            }
            
            @Override
            public Expr resolveAlias(final String alias) {
                return SelectCompiler.this.alias2expr.get(alias);
            }
        };
        return new ExprValidationVisitor(ctx).validate(e);
    }
    
    private ValueExpr validateValueExpr(final ValueExpr e, final boolean canHaveAggFunc, final boolean acceptWildcard) {
        return (ValueExpr)this.validateExpr(e, canHaveAggFunc, acceptWildcard);
    }
    
    private Predicate validatePredicate(final Predicate e, final boolean canHaveAggFunc) {
        final Predicate p = (Predicate)this.validateExpr(e, canHaveAggFunc, false);
        if (!canHaveAggFunc) {
            this.predicates.add(p);
        }
        return p;
    }
    
    private void addCondExpr(final Condition.CondExpr e, final Join.JoinNode jn) {
        this.condIndex.add(e);
    }
    
    private void addCondExpr(final Map<Condition.CondKey, Condition> map, final Condition.CondExpr e) {
        final Condition.CondKey key = e.getCondKey();
        if (map.containsKey(key)) {
            map.get(key).exprs.add(e);
        }
        else {
            final Condition cond = new Condition(e);
            map.put(key, cond);
        }
    }
    
    private void addFilter(final Map<DataSet, Condition> map, final Condition.CondExpr e) {
        final BitSet datasets = e.datasets;
        assert datasets.cardinality() == 1;
        final int datasetID = datasets.nextSetBit(0);
        final DataSet ds = this.getDataSet(datasetID);
        if (map.containsKey(ds)) {
            map.get(ds).exprs.add(e);
        }
        else {
            final Condition cond = new Condition(e);
            map.put(ds, cond);
        }
    }
    
    private void analyzeAndPredicate(final Predicate e, final Join.JoinNode jn) {
        if (e instanceof LogicalPredicate && e.op == ExprCmd.AND) {
            final LogicalPredicate lp = (LogicalPredicate)e;
            for (final Predicate p : lp.args) {
                this.analyzeAndPredicate(p, jn);
            }
        }
        else {
            this.analyzePredicate(e, jn);
        }
    }
    
    private void analyzePredicates() {
        if (this.having != null && this.select.groupBy == null && !this.haveAggFuncs) {
            this.predicates.add(this.having);
            this.having = null;
        }
        for (final Predicate e : this.predicates) {
            final Join.JoinNode jn = this.joinTree.getJoinPredicate(e);
            this.analyzeAndPredicate(e, jn);
        }
    }
    
    private void analyzePredicate(Predicate p, final Join.JoinNode jn) {
        final FindCommonSubexpression fcs = this.makeCommonSubexprRewriter();
        p = fcs.rewrite(p);
        final ConditionAnalyzer ca = new ConditionAnalyzer(p, jn);
        final Condition.CondExpr e = ca.getCondExpr();
        this.addCondExpr(e, jn);
        switch (e.datasets.cardinality()) {
            case 0: {
                this.constantConditions.add(e);
                break;
            }
            case 1: {
                this.addFilter(this.filterConditions, e);
                break;
            }
            default: {
                if (e instanceof Condition.JoinExpr) {
                    this.addCondExpr(this.joinConditions, e);
                    break;
                }
                this.addCondExpr(this.afterJoinFilterConditions, e);
                break;
            }
        }
    }
    
    private static void dumpList(final PrintStream out, final String what, final Collection<?> l) {
        if (!l.isEmpty()) {
            out.println("------------------------------------------");
            out.println(what);
            for (final Object o : l) {
                out.println("\t" + o);
            }
        }
    }
    
    private void dumpConditions(final PrintStream out) {
        dumpList(out, "list of constant conditions", this.constantConditions);
        dumpList(out, "list of filter conditions", this.filterConditions.values());
        dumpList(out, "list of join conditions", this.joinConditions.values());
        dumpList(out, "list of after join filter conditions", this.afterJoinFilterConditions.values());
        dumpList(out, "list of before agg expressions", this.exprIndex.values());
        dumpList(out, "list of eliminated expressions", this.eliminated.values());
    }
    
    private void dumpDataSets(final PrintStream out) {
        out.println("list of datasets");
        for (final DataSet ds : this.dataSets) {
            out.println(ds);
        }
    }
    
    private void dumpIndexes(final PrintStream out) {
        out.println("\nList of indexes for every dataset\n");
        for (final Map.Entry<DataSet, List<IndexDesc>> e : this.indexes.entrySet()) {
            final DataSet ds = e.getKey();
            out.println("dataset: " + ds.getName() + " {" + ds.getID() + "}");
            for (final IndexDesc id : e.getValue()) {
                out.println("-- index: " + id);
            }
        }
    }
    
    private void dumpJoinPlanEveryDataSet(final PrintStream out) {
        out.println("\nList of join plans for every dataset\n");
        for (final Map.Entry<DataSet, List<JoinDesc>> e : this.joinPlans.entrySet()) {
            final DataSet ds = e.getKey();
            out.println("-----------------------------------------------------");
            out.println("dataset: " + ds.getName() + " {" + ds.getID() + "}");
            out.println("------- join plan -----------------------------------");
            for (final JoinDesc jd : e.getValue()) {
                out.println(jd);
            }
        }
    }
    
    private void analyzeIndexedCondtions() {
        for (final Map.Entry<DataSet, Condition> e : this.filterConditions.entrySet()) {
            final DataSet ds = e.getKey();
            final Condition cond = e.getValue();
            final List<Pair<FieldRef, ValueExpr>> l = cond.getEqExprs();
            final Set<String> names = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            for (final Pair<FieldRef, ValueExpr> p : l) {
                final String fieldName = p.first.getName();
                names.add(fieldName);
            }
            final List<Integer> lst = ds.getListOfIndexesForFields(names);
            if (!lst.isEmpty()) {
                final int indexID = lst.get(0);
                final List<String> indexedFields = ds.indexFieldList(indexID);
                final List<ValueExpr> key = new ArrayList<ValueExpr>();
                for (final String indexField : indexedFields) {
                    for (final Pair<FieldRef, ValueExpr> p2 : l) {
                        final String fieldName2 = p2.first.getName();
                        if (fieldName2.equalsIgnoreCase(indexField)) {
                            key.add(p2.second);
                        }
                    }
                }
                this.index0key.put(ds, key);
            }
        }
    }
    
    private void buildJoinPlan() {
        assert this.dataSets.size() > 0;
        if (this.dataSets.size() == 1) {
            assert this.joinConditions.isEmpty();
            assert this.afterJoinFilterConditions.isEmpty();
        }
        else {
            List<Condition> joinConds = new ArrayList<Condition>(this.joinConditions.values());
            for (final Condition c : joinConds) {
                final Map<Integer, Set<String>> flds = c.getCondFields();
                for (final Map.Entry<Integer, Set<String>> e : flds.entrySet()) {
                    final int dsID = e.getKey();
                    final Set<String> set = e.getValue();
                    final DataSet ds = this.getDataSet(e.getKey());
                    final List<Integer> lst = ds.getListOfIndexesForFields(set);
                    if (!lst.isEmpty()) {
                        c.updateFieldIndexesSupport(dsID, lst);
                    }
                }
            }
            Collections.sort(joinConds);
            joinConds = Collections.unmodifiableList((List<? extends Condition>)joinConds);
            List<Condition> afterJoinConds = new ArrayList<Condition>(this.afterJoinFilterConditions.values());
            Collections.sort(afterJoinConds);
            afterJoinConds = Collections.unmodifiableList((List<? extends Condition>)afterJoinConds);
            final BitSet all = new BitSet();
            all.set(0, this.dataSets.size());
            for (int i = all.nextSetBit(0); i >= 0; i = all.nextSetBit(i + 1)) {
                final BitSet bs = (BitSet)all.clone();
                bs.clear(i);
                this.makeJoinPlan(i, bs, joinConds, afterJoinConds);
            }
        }
    }
    
    private void checkComparable(final Expr keyExpr) {
        final Class<?> t = keyExpr.getType();
        if (!t.isPrimitive() && !Comparable.class.isAssignableFrom(t)) {
            this.error("non-comparable expression", keyExpr);
        }
    }
    
    private void makeJoinPlan(final int firstdsID, final BitSet leftToJoin, final List<Condition> _joinConds, final List<Condition> _afterJoinConds) {
        final DataSet firstDS = this.getDataSet(firstdsID);
        final BitSet alreadyJoined = new BitSet();
        alreadyJoined.set(firstdsID);
        final BitSet check = (BitSet)alreadyJoined.clone();
        check.or(leftToJoin);
        final LinkedList<Condition> joinConds = new LinkedList<Condition>(_joinConds);
        final LinkedList<Condition> afterJoinConds = new LinkedList<Condition>(_afterJoinConds);
        final List<JoinDesc> joinPlan = new ArrayList<JoinDesc>();
        while (!leftToJoin.isEmpty()) {
            boolean isOuter = false;
            Condition joinCond = null;
            List<Expr> searchExprs = Collections.emptyList();
            IndexDesc idx = null;
            List<Predicate> postFilters = Collections.emptyList();
            int windowIndexID = -1;
            int datasetID = this.findNextStreamJoin(leftToJoin);
            JoinDesc.JoinAlgorithm algo;
            DataSet nextDS;
            if (datasetID >= 0) {
                algo = JoinDesc.JoinAlgorithm.WITH_STREAM;
                nextDS = this.getDataSet(datasetID);
            }
            else {
                final Pair<Integer, Condition> ic = this.findBestIndexedJoinCondition(alreadyJoined, joinConds);
                if (ic != null) {
                    algo = JoinDesc.JoinAlgorithm.WINDOW_INDEX_LOOKUP;
                    datasetID = ic.first;
                    nextDS = this.getDataSet(datasetID);
                    joinCond = ic.second;
                    windowIndexID = joinCond.dsIndexes.get(datasetID).get(0);
                    final Pair<List<Expr>, List<Predicate>> searchAndFilter = joinCond.getIndexSearchAndPostCondition(datasetID, nextDS.indexFieldList(windowIndexID));
                    searchExprs = searchAndFilter.first;
                    postFilters = searchAndFilter.second;
                }
                else {
                    final Pair<Integer, Condition> p = null;
                    if (p != null) {
                        algo = JoinDesc.JoinAlgorithm.INDEX_LOOKUP;
                        datasetID = p.first;
                        nextDS = this.getDataSet(datasetID);
                        joinCond = p.second;
                        searchExprs = joinCond.getSearchExprs(datasetID);
                        final List<Expr> indexExprs = joinCond.getIndexExprs(datasetID);
                        idx = this.getIndexDesc(this.getDataSet(datasetID), indexExprs);
                    }
                    else {
                        algo = JoinDesc.JoinAlgorithm.SCAN;
                        datasetID = this.findBestCrossJoinCondition(alreadyJoined, leftToJoin, afterJoinConds);
                        nextDS = this.getDataSet(datasetID);
                    }
                }
            }
            isOuter = this.joinTree.isOuterJoin(alreadyJoined, datasetID, this.condIndex);
            alreadyJoined.set(datasetID);
            leftToJoin.clear(datasetID);
            assert alreadyJoined.cardinality() > 1;
            final JoinDesc desc = new JoinDesc(algo, alreadyJoined, nextDS, joinCond, idx, searchExprs, postFilters, windowIndexID, isOuter);
            joinPlan.add(desc);
        }
        assert alreadyJoined.equals(check);
        afterJoinConds.addAll(0, joinConds);
        for (final Map.Entry<DataSet, Condition> e : this.filterConditions.entrySet()) {
            if (e.getKey().getID() != firstdsID) {
                afterJoinConds.add(0, e.getValue());
            }
        }
        alreadyJoined.clear();
        alreadyJoined.set(firstdsID);
        for (final JoinDesc jd : joinPlan) {
            assert jd.joinSet.cardinality() > 1;
            assert alreadyJoined.cardinality() + 1 == jd.joinSet.cardinality();
            alreadyJoined.or(jd.joinSet);
            this.addAfterJoinCondtions(jd, alreadyJoined, afterJoinConds);
        }
        assert afterJoinConds.isEmpty();
        assert !this.joinPlans.containsKey(firstDS);
        this.joinPlans.put(firstDS, joinPlan);
    }
    
    private int findNextStreamJoin(final BitSet leftToJoin) {
        for (int i = leftToJoin.nextSetBit(0); i >= 0; i = leftToJoin.nextSetBit(i + 1)) {
            final DataSet ds = this.getDataSet(i);
            if (!ds.isStateful()) {
                return i;
            }
        }
        return -1;
    }
    
    private Pair<Integer, Condition> findBestIndexedJoinCondition(final BitSet alreadyJoined, final LinkedList<Condition> joinConds) {
        final ListIterator<Condition> it = joinConds.listIterator();
        while (it.hasNext()) {
            final Condition cond = it.next();
            if (cond.idxCount > 0) {
                final BitSet set = (BitSet)cond.dataSets.clone();
                set.andNot(alreadyJoined);
                if (set.cardinality() != 1) {
                    continue;
                }
                final int relID = set.nextSetBit(0);
                final List<Integer> indexes = cond.dsIndexes.get(relID);
                if (indexes != null) {
                    it.remove();
                    return Pair.make(relID, cond);
                }
                continue;
            }
        }
        return null;
    }
    
    private Pair<Integer, Condition> findBestInnerJoinCondition(final BitSet alreadyJoined, final LinkedList<Condition> joinConds) {
        final ListIterator<Condition> it = joinConds.listIterator();
        while (it.hasNext()) {
            final Condition cond = it.next();
            if (cond.isJoinable()) {
                final BitSet set = (BitSet)cond.dataSets.clone();
                set.andNot(alreadyJoined);
                if (set.cardinality() != 1) {
                    continue;
                }
                final int relID = set.nextSetBit(0);
                if (cond.isIndexable(relID)) {
                    it.remove();
                    return Pair.make(relID, cond);
                }
                continue;
            }
        }
        return null;
    }
    
    private boolean derivedHasParentJoined(final int id, final BitSet alreadyJoined) {
        final DataSet ds = this.getDataSet(id);
        final IteratorInfo it = ds.isIterator();
        if (it == null) {
            return true;
        }
        final DataSet parent = it.getParent();
        return alreadyJoined.get(parent.getID());
    }
    
    private int findBestCrossJoinCondition(final BitSet alreadyJoined, final BitSet leftToJoin, final List<Condition> afterJoinConds) {
        final BitSet left = (BitSet)leftToJoin.clone();
        for (final Condition cond : afterJoinConds) {
            final BitSet set = (BitSet)cond.dataSets.clone();
            set.andNot(alreadyJoined);
            if (set.cardinality() == 1) {
                final int id = set.nextSetBit(0);
                if (this.derivedHasParentJoined(id, alreadyJoined)) {
                    return id;
                }
            }
            left.and(set);
        }
        int id2 = 0;
        Label_0143: {
            if (!left.isEmpty()) {
                do {
                    id2 = left.nextSetBit(id2);
                    if (id2 == -1) {
                        id2 = 0;
                        break Label_0143;
                    }
                } while (!this.derivedHasParentJoined(id2, alreadyJoined));
                return id2;
            }
            do {
                id2 = leftToJoin.nextSetBit(id2);
                if (id2 == -1) {
                    assert false;
                    return -1;
                }
            } while (!this.derivedHasParentJoined(id2, alreadyJoined));
        }
        return id2;
    }
    
    private IndexDesc getIndexDesc(final DataSet ds, final List<Expr> indexExprs) {
        List<IndexDesc> ilist;
        if (this.indexes.containsKey(ds)) {
            ilist = this.indexes.get(ds);
            for (final IndexDesc id : ilist) {
                if (id.exprs.equals(indexExprs)) {
                    return id;
                }
            }
        }
        else {
            ilist = new ArrayList<IndexDesc>();
            this.indexes.put(ds, ilist);
        }
        final IndexDesc id2 = new IndexDesc(this.nextIndexID++, ds, indexExprs);
        ilist.add(id2);
        return id2;
    }
    
    private void addAfterJoinCondtions(final JoinDesc jd, final BitSet alreadyJoined, final LinkedList<Condition> afterJoinConds) {
        final ListIterator<Condition> it = afterJoinConds.listIterator();
        while (it.hasNext()) {
            final Condition cond = it.next();
            final BitSet set = (BitSet)cond.dataSets.clone();
            set.andNot(alreadyJoined);
            if (set.isEmpty()) {
                it.remove();
                jd.afterJoinConditions.add(cond);
            }
        }
    }
    
    private FindCommonSubexpression makeCommonSubexprRewriter() {
        final FindCommonSubexpression.Rewriter rewriter = new FindCommonSubexpression.Rewriter() {
            @Override
            public Expr rewriteExpr(final Expr e) {
                final Expr commonSubExpr = SelectCompiler.this.exprIndex.get(e);
                if (commonSubExpr == null) {
                    SelectCompiler.this.exprIndex.put(e, e);
                    return e;
                }
                if (!SelectCompiler.this.eliminated.containsKey(e.eid)) {
                    SelectCompiler.this.eliminated.put(e.eid, e);
                }
                return commonSubExpr;
            }
            
            @Override
            public void addAggrExpr(final FuncCall aggFuncCall) {
                if (!SelectCompiler.this.aggregatedExprs.contains(aggFuncCall)) {
                    final int index = SelectCompiler.this.aggregatedExprs.size();
                    SelectCompiler.this.aggregatedExprs.add(aggFuncCall);
                    aggFuncCall.setIndex(index);
                }
            }
        };
        return new FindCommonSubexpression(rewriter) {};
    }
    
    private void analyzeGropyBy() {
        if (this.select.groupBy != null) {
            final FindCommonSubexpression fcs = this.makeCommonSubexprRewriter();
            final ListIterator<ValueExpr> it = this.groupBy.listIterator();
            while (it.hasNext()) {
                final ValueExpr e = it.next();
                final ValueExpr enew = fcs.rewrite(e);
                if (enew != null) {
                    it.set(enew);
                }
            }
        }
    }
    
    private void analyzeOrderBy() {
        if (this.select.orderBy != null) {
            final FindCommonSubexpression fcs = this.makeCommonSubexprRewriter();
            final ListIterator<OrderByItem> it = this.orderBy.listIterator();
            while (it.hasNext()) {
                final OrderByItem e = it.next();
                final ValueExpr enew = fcs.rewrite(e.expr);
                if (enew != null) {
                    it.set(new OrderByItem(enew, e.isAscending));
                }
            }
        }
    }
    
    private List<FieldRef> makeListOfAllFields() {
        final List<FieldRef> allFields = new ArrayList<FieldRef>();
        for (final DataSet ds : this.dataSets) {
            final List<FieldRef> fields = ds.makeListOfAllFields();
            allFields.addAll(fields);
        }
        return allFields;
    }
    
    private void analyzeHavingAndTargets() {
        final FindCommonSubexpression fcs = this.makeCommonSubexprRewriter();
        if (this.having != null) {
            this.having = fcs.rewrite(this.having);
        }
        for (final Target t : this.targets) {
            ValueExpr enew = fcs.rewrite(t.expr);
            if (CompilerUtils.isParam(enew.getType())) {
                enew = this.cast(enew, String.class);
            }
            t.expr = enew;
        }
    }
    
    public static class ModifyTarget
    {
        public Integer index;
        public ValueExpr expr;
        
        public ModifyTarget(final Integer index, final ValueExpr expr) {
            assert index != null;
            this.index = index;
            this.expr = expr;
        }
        
        @Override
        public String toString() {
            return "Modify Target(" + this.expr + " to the data field " + this.index + ")";
        }
    }
    
    public static class Target
    {
        public ValueExpr expr;
        public String alias;
        public Field targetField;
        
        public Target(final ValueExpr expr, final String alias) {
            assert alias != null;
            this.expr = expr;
            this.alias = alias;
            this.targetField = null;
        }
        
        @Override
        public String toString() {
            return "Target(" + this.expr + " as " + this.alias + "->" + this.targetField + ")";
        }
    }
}
