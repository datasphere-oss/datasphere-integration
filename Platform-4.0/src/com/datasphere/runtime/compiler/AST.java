package com.datasphere.runtime.compiler;

import com.datasphere.drop.*;
import com.datasphere.kafkamessaging.*;
import com.datasphere.runtime.components.*;
import com.datasphere.runtime.compiler.exprs.*;
import java.util.*;
import java.io.*;
import javax.validation.constraints.*;
import javax.annotation.*;
import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.*;
import com.datasphere.utility.*;
import com.datasphere.security.*;
import com.datasphere.metaRepository.*;
import scala.xml.parsing.*;
import com.datasphere.runtime.meta.*;
import com.datasphere.runtime.compiler.patternmatch.*;
import com.datasphere.runtime.compiler.stmts.*;

public class AST
{
    public final Grammar parser;
    
    private void error(final String msg, final Object obj) {
        this.parser.parseError(msg, obj);
    }
    
    private static String getExprText(final Object obj, final Grammar p) {
        return (p == null) ? "" : p.lex.getExprText(obj);
    }
    
    public static <T> List<T> NewList(final T first_item) {
        return new ArrayList<T>((Collection<? extends T>)Collections.singletonList(first_item));
    }
    
    public static <T> List<T> NewList(final T first_item, final T second_item) {
        return new ArrayList<T>((Collection<? extends T>)Arrays.asList(first_item, second_item));
    }
    
    public static <T> List<T> NewList(final List<T> list, final T next_item) {
        final List<T> newlist = new ArrayList<T>((Collection<? extends T>)list);
        newlist.add(next_item);
        return newlist;
    }
    
    public static <T> List<T> AddToList(final List<T> list, final T next_item) {
        list.add(next_item);
        return list;
    }
    
    public static MappedStream CreateMappedStream(final String streamName, final List<Property> mappingProps) {
        return new MappedStream(streamName, mappingProps);
    }
    
    public AST(final Grammar parser) {
        this.parser = parser;
    }
    
    public Interval ParseDSInterval(final String literal, final int flags) {
        final Interval i = Interval.parseDSInterval(literal, flags);
        if (i == null) {
            this.error("invalid interval literal", literal);
        }
        return i;
    }
    
    public Interval ParseYMInterval(final String literal, final int flags) {
        final Interval i = Interval.parseYMInterval(literal, flags);
        if (i == null) {
            this.error("invalid interval literal", literal);
        }
        return i;
    }
    
    public static Interval makeDSInterval(final double val) {
        final long l = Math.round(val * 1000000.0);
        return new Interval(l);
    }
    
    public static Interval makeDSInterval(final long val, final int flags) {
        switch (flags) {
            case 1: {
                return new Interval(val * 1000000L);
            }
            case 2: {
                return new Interval(val * 1000000L * 60L);
            }
            case 4: {
                return new Interval(val * 1000000L * 60L * 60L);
            }
            case 8: {
                return new Interval(val * 1000000L * 60L * 60L * 24L);
            }
            default: {
                assert false;
                return null;
            }
        }
    }
    
    public static Interval makeYMInterval(final long val, final int flags) {
        switch (flags) {
            case 16: {
                return new Interval(val);
            }
            case 32: {
                return new Interval(val * 12L);
            }
            default: {
                assert false;
                return null;
            }
        }
    }
    
    public static TypeName CreateType(final String typename, final int array_dimensions) {
        return new TypeName(typename, array_dimensions);
    }
    
    public static ValueExpr NewNumericExpr(final ExprCmd op, final ValueExpr left, final ValueExpr right) {
        if (left instanceof NumericOperation && left.op == op) {
            final NumericOperation o = (NumericOperation)left;
            return new NumericOperation(op, NewList(o.args, right));
        }
        return new NumericOperation(op, NewList(left, right));
    }
    
    public static ValueExpr NewIntegerExpr(final ExprCmd op, final ValueExpr left, final ValueExpr right) {
        if (left instanceof IntegerOperation && left.op == op) {
            final IntegerOperation o = (IntegerOperation)left;
            return new IntegerOperation(op, NewList(o.args, right));
        }
        return new IntegerOperation(op, NewList(left, right));
    }
    
    public static ValueExpr NewUnaryNumericExpr(final ExprCmd op, final ValueExpr expr) {
        return new NumericOperation(op, NewList(expr));
    }
    
    public static ValueExpr NewUnaryIntegerExpr(final ExprCmd op, final ValueExpr expr) {
        return new IntegerOperation(op, NewList(expr));
    }
    
    public static ValueExpr NewIntegerConstant(final Integer literal) {
        return Constant.newInt(literal);
    }
    
    public static ValueExpr NewLongConstant(final Long literal) {
        return Constant.newLong(literal);
    }
    
    public static ValueExpr NewFloatConstant(final Float literal) {
        return Constant.newFloat(literal);
    }
    
    public static ValueExpr NewDoubleConstant(final Double literal) {
        return Constant.newDouble(literal);
    }
    
    public static ValueExpr NewNullConstant() {
        return Constant.newNull();
    }
    
    public static ValueExpr NewBoolConstant(final boolean value) {
        return Constant.newBool(value);
    }
    
    public static ValueExpr NewDateConstant(final String literal) {
        return Constant.newDate(literal);
    }
    
    public static ValueExpr NewTimestampConstant(final String literal) {
        return Constant.newDate(literal);
    }
    
    public static ValueExpr NewDateTimeConstant(final String literal) {
        return Constant.newDateTime(literal);
    }
    
    public static ValueExpr NewYMIntervalConstant(final Interval value) {
        return Constant.newYMInterval(value);
    }
    
    public static ValueExpr NewDSIntervalConstant(final Interval value) {
        return Constant.newDSInterval(value);
    }
    
    public static ValueExpr NewSimpleCaseExpression(final ValueExpr selector, final List<Case> caselist, final ValueExpr opt_else) {
        return new CaseExpr(selector, caselist, opt_else);
    }
    
    public static ValueExpr NewSearchedCaseExpression(final List<Case> caselist, final ValueExpr opt_else) {
        return new CaseExpr(null, caselist, opt_else);
    }
    
    public static Predicate NewInstanceOfExpr(final ValueExpr expr, final TypeName type) {
        return new InstanceOfPredicate(expr, type);
    }
    
    public static Predicate NewCompareExpr(final ExprCmd op, final ValueExpr left, final ValueExpr right) {
        return new ComparePredicate(op, NewList(left, right));
    }
    
    public static ModifyExpr modifyExpr(final ExprCmd op, final ValueExpr left, final SelectTarget right) {
        final ModifyExpr modifyExpr = new ModifyExpr();
        modifyExpr.op = op;
        modifyExpr.args.add(left);
        modifyExpr.args.add(right);
        return modifyExpr;
    }
    
    public static Predicate NewLikeExpr(final ValueExpr expr, final boolean not, final ValueExpr pattern) {
        final Predicate p = new ComparePredicate(ExprCmd.LIKE, NewList(expr, pattern));
        return not ? NewNotExpr(p) : p;
    }
    
    public static Predicate NewBetweenExpr(final ValueExpr expr, final boolean not, final ValueExpr lowerbound, final ValueExpr upperbound) {
        final List<ValueExpr> l = new ArrayList<ValueExpr>(3);
        Collections.addAll(l, new ValueExpr[] { expr, lowerbound, upperbound });
        final Predicate p = new ComparePredicate(ExprCmd.BETWEEN, l);
        return not ? NewNotExpr(p) : p;
    }
    
    public static Predicate NewIsNullExpr(final ValueExpr expr, final boolean not) {
        final Predicate p = new ComparePredicate(ExprCmd.ISNULL, NewList(expr));
        return not ? NewNotExpr(p) : p;
    }
    
    public static Predicate NewInListExpr(final ValueExpr expr, final boolean not, List<ValueExpr> list) {
        if (list == null) {
            list = Collections.emptyList();
        }
        final List<ValueExpr> l = new ArrayList<ValueExpr>(1 + list.size());
        l.add(expr);
        l.addAll(list);
        final Predicate p = new ComparePredicate(ExprCmd.INLIST, l);
        return not ? NewNotExpr(p) : p;
    }
    
    public static Predicate NewOrExpr(final Predicate left, final Predicate right) {
        if (left instanceof LogicalPredicate && left.op == ExprCmd.OR) {
            final LogicalPredicate p = (LogicalPredicate)left;
            return new LogicalPredicate(ExprCmd.OR, NewList(p.args, right));
        }
        return new LogicalPredicate(ExprCmd.OR, NewList(left, right));
    }
    
    public static Predicate NewAndExpr(final Predicate left, final Predicate right) {
        if (left instanceof LogicalPredicate && left.op == ExprCmd.AND) {
            final LogicalPredicate p = (LogicalPredicate)left;
            return new LogicalPredicate(ExprCmd.AND, NewList(p.args, right));
        }
        return new LogicalPredicate(ExprCmd.AND, NewList(left, right));
    }
    
    public static Predicate NewNotExpr(final Predicate expr) {
        return new LogicalPredicate(ExprCmd.NOT, NewList(expr));
    }
    
    public static FuncArgs NewFuncArgs(List<ValueExpr> args, final int options) {
        if (args == null) {
            args = new ArrayList<ValueExpr>();
        }
        return new FuncArgs(args, options);
    }
    
    public static ValueExpr NewStringConstant(final String val) {
        return Constant.newString(val);
    }
    
    public static ValueExpr NewObjectConstructorExpr(final String typeName, List<ValueExpr> args) {
        if (args == null) {
            args = Collections.emptyList();
        }
        final TypeName tn = new TypeName(typeName, 0);
        return new ConstructorCall(tn, args);
    }
    
    public static ValueExpr NewArryConstructorExpr(final String typeName, final List<ValueExpr> indexes, final int brackets) {
        final TypeName tn = new TypeName(typeName, indexes.size() + brackets);
        return new ArrayConstructor(tn, indexes);
    }
    
    public static ValueExpr NewCastExpr(final ValueExpr e, final TypeName t) {
        return new CastExpr(t, e);
    }
    
    public static ValueExpr NewIdentifierRef(final String name) {
        return new ObjectRef(name);
    }
    
    public static ValueExpr NewFuncCall(final String funcName, final FuncArgs args) {
        return new FuncCall(funcName, args.args, args.options);
    }
    
    private void checkValidPredecessor(final ValueExpr e) {
        if (e instanceof ArrayTypeRef) {
            this.error("invalid expression", e);
        }
    }
    
    public ValueExpr NewFieldRef(final ValueExpr e, final String fieldName) {
        this.checkValidPredecessor(e);
        return FieldRef.createUnresolvedFieldRef(e, fieldName);
    }
    
    public ValueExpr NewMethodCall(final ValueExpr e, final String funcName, final FuncArgs args) {
        this.checkValidPredecessor(e);
        args.args.add(0, e);
        return new MethodCall(funcName, args.args, args.options);
    }
    
    public static ValueExpr NewIndexExpr(final ValueExpr e, final List<ValueExpr> indexes) {
        ValueExpr res = e;
        for (final ValueExpr idx : indexes) {
            final ValueExpr tmp = new IndexExpr(idx, res);
            res.setOriginalExpr(tmp);
            res = tmp;
        }
        return res;
    }
    
    public ValueExpr NewArrayTypeRef(final ValueExpr e, final String typeName, final int brackets) {
        String name = this.getClassNamePrefix(null, e);
        if (name == null) {
            this.error("invalid type name", e);
        }
        name = name + "." + typeName;
        final TypeName tn = new TypeName(name, brackets);
        return new ArrayTypeRef(tn);
    }
    
    public static ValueExpr NewArrayTypeRef(final String typeName, final int brackets) {
        final TypeName tn = new TypeName(typeName, brackets);
        return new ArrayTypeRef(tn);
    }
    
    private String getClassNamePrefix(String name, final ValueExpr e) {
        if (e instanceof ObjectRef) {
            final ObjectRef o = (ObjectRef)e;
            name = o.name + ((name == null) ? "" : ("." + name));
            return name;
        }
        if (e instanceof FieldRef) {
            final FieldRef f = (FieldRef)e;
            name = f.getName() + ((name == null) ? "" : ("." + name));
            return this.getClassNamePrefix(name, f.getExpr());
        }
        return null;
    }
    
    public ValueExpr NewClassRef(final ValueExpr e) {
        if (e instanceof ArrayTypeRef) {
            final ArrayTypeRef a = (ArrayTypeRef)e;
            return new ClassRef(a.typeName);
        }
        final String name = this.getClassNamePrefix(null, e);
        if (name == null) {
            this.error("invalid type name", e);
        }
        final TypeName tn = new TypeName(name.toString(), 0);
        return new ClassRef(tn);
    }
    
    public ValueExpr NewDataSetAllFields(final ValueExpr e) {
        if (e instanceof ObjectRef) {
            final ObjectRef o = (ObjectRef)e;
            return new WildCardExpr(o.name);
        }
        this.error("invalid syntax", e);
        return e;
    }
    
    public ValueExpr NewParameterRef(final String pname) {
        return new ParamRef(pname);
    }
    
    public static Stmt ImportClassDeclaration(final String pkgname, final boolean isStatic) {
        return new ImportStmt(pkgname, false, isStatic);
    }
    
    public static Stmt ImportPackageDeclaration(final String pkgname, final boolean isStatic) {
        return new ImportStmt(pkgname, true, isStatic);
    }
    
    public static Stmt CreateCqStatement(final String cq_name, final Boolean doReplace, final String dest_stream_name, List<String> field_name_list, final Select select, final String selectText) {
        if (dest_stream_name == null) {
            return CreateAdHocSelectStmt(select, selectText, cq_name);
        }
        if (field_name_list == null) {
            field_name_list = Collections.emptyList();
        }
        return new CreateCqStmt(cq_name, doReplace, dest_stream_name, field_name_list, select, selectText);
    }
    
    public static Stmt CreateCqStatement(final String cq_name, final Boolean doReplace, final String dest_stream_name, final List<String> field_name_list, final Select select, final String selectText, final String uiConfig) {
        return new CreateCqStmt(cq_name, doReplace, dest_stream_name, field_name_list, select, selectText, uiConfig);
    }
    
    public static Stmt CreateCqStatement(final String cq_name, final Boolean doReplace, final String dest_stream_name, List<String> field_name_list, final List<String> partition_fields, final StreamPersistencePolicy persistencePolicy, final Select select, final String selectText) {
        if (dest_stream_name == null) {
            throw new RuntimeException("Not correct usage.");
        }
        if (field_name_list == null) {
            field_name_list = Collections.emptyList();
        }
        final CreateCqStmt stmt = new CreateCqStmt(cq_name, doReplace, dest_stream_name, field_name_list, select, selectText);
        stmt.setPartitionFieldList(partition_fields);
        stmt.setPersistencePolicy(persistencePolicy);
        return stmt;
    }
    
    public static Stmt CreateCqStatement(final String cq_name, final Boolean doReplace, final String dest_stream_name, final List<String> field_name_list, final Select select, final Grammar p) {
        final String selectText = getExprText(select, p);
        return CreateCqStatement(cq_name, doReplace, dest_stream_name, field_name_list, select, selectText);
    }
    
    public static Stmt CreateCqStatement(final String cq_name, final Boolean doReplace, final String dest_stream_name, final List<String> field_name_list, final List<String> partition_fields, final StreamPersistencePolicy persistencePolicy, final Select select, final Grammar p) {
        String selectText = getExprText(select, p);
        final String modifyString = getExprText(select.modify, p);
        if (modifyString != null) {
            selectText = selectText + "\n" + modifyString;
        }
        return CreateCqStatement(cq_name, doReplace, dest_stream_name, field_name_list, partition_fields, persistencePolicy, select, selectText);
    }
    
    public static Stmt CreateStreamStatement(final String stream_name, final Boolean doReplace, final List<String> partition_fields, final TypeDefOrName typedef, final GracePeriod gp, final StreamPersistencePolicy spp) {
        return new CreateStreamStmt(stream_name, doReplace, partition_fields, typedef.typeName, typedef.typeDef, gp, spp);
    }
    
    public static Stmt CreateTypeStatement(final String type_name, final Boolean doReplace, final TypeDefOrName typeDef) {
        return new CreateTypeStmt(type_name, doReplace, typeDef);
    }
    
    public static Stmt CreateWindowStatement(final String window_name, final Boolean doReplace, final String stream_name, final Pair<IntervalPolicy, IntervalPolicy> window_len, final boolean isJumping, final List<String> partition_fields) {
        return new CreateWindowStmt(window_name, doReplace, stream_name, window_len, isJumping, partition_fields, null);
    }
    
    public static Stmt DropStatement(final EntityType objectType, final String objectName, final DropMetaObject.DropRule dropRule) {
        return new DropStmt(objectType, objectName, dropRule);
    }
    
    public static DataSource Join(final DataSource left, final DataSource right, final Join.Kind kindOfJoin, final Predicate joinCond) {
        return new DataSourceJoin(left, right, kindOfJoin, joinCond);
    }
    
    public static OutputClause newOutputClause(final String stream, final MappedStream mp, final List<String> part, final List<TypeField> fields, final Select select, final Grammar parser) {
        return new OutputClause(stream, mp, part, fields, select, getExprText(select, parser));
    }
    
    public static Select CreateSelect(final boolean distinct, final int kind, final List<SelectTarget> targets, final List<DataSource> from, final Predicate where, final List<ValueExpr> group, final Predicate having, final List<OrderByItem> orderby, final LimitClause limit, final boolean linksrc) {
        return new Select(distinct, kind, targets, from, where, group, having, orderby, limit, linksrc, null);
    }
    
    public static SelectTarget SelectTarget(final Expr expr, String alias, final Grammar p) {
        if (alias == null) {
            alias = getExprText(expr, p);
        }
        assert alias != null;
        ValueExpr valexpr;
        if (expr instanceof Predicate) {
            if (expr.op == ExprCmd.BOOLEXPR) {
                valexpr = ((ComparePredicate)expr).args.get(0);
            }
            else {
                valexpr = new PredicateAsValueExpr((Predicate)expr);
                valexpr.setOriginalExpr(expr);
            }
        }
        else {
            valexpr = (ValueExpr)expr;
        }
        return new SelectTarget(valexpr, alias);
    }
    
    public static SelectTarget SelectTargetAll() {
        return new SelectTarget(null, null);
    }
    
    public static DataSource SourceStream(final String stream_name, final String opt_typename, final String alias) {
        return new DataSourceStream(stream_name, opt_typename, alias);
    }
    
    public static DataSource SourceView(final Select subSelect, final String alias, final Grammar p) {
        final String selectText = getExprText(subSelect, p);
        return new DataSourceView(subSelect, alias, selectText);
    }
    
    public static DataSource SourceStreamFunction(final String funcName, List<ValueExpr> args, final String alias) {
        if (args == null) {
            args = Collections.emptyList();
        }
        return new DataSourceStreamFunction(funcName, args, alias);
    }
    
    public static TypeField TypeField(final String name, final TypeName type, final boolean iskey) {
        return new TypeField(name, type, iskey);
    }
    
    public static Stmt CreateUseNamespaceStmt(final String schemaName) {
        return new UseStmt(EntityType.NAMESPACE, schemaName, false);
    }
    
    public static Stmt CreateAlterAppOrFlowStmt(final EntityType type, final String name, final boolean recompile) {
        return new UseStmt(type, name, recompile);
    }
    
    public static Property CreateProperty(final String n, final Object v) {
        return new Property(n, v);
    }
    
    public static Stmt CreateSourceStatement(final String n, final Boolean r, final AdapterDescription src, final AdapterDescription parser, final List<OutputClause> oc) {
        final List<InputOutputSink> ll = new ArrayList<InputOutputSink>();
        ll.addAll(oc);
        return new CreateSourceOrTargetStmt(EntityType.SOURCE, n, r, src, parser, ll);
    }
    
    public static Stmt CreateTargetStatement(final String n, final Boolean r, final AdapterDescription dest, final AdapterDescription formatter, final String stream, final List<String> partition_fields) {
        final List<InputOutputSink> oc = new ArrayList<InputOutputSink>();
        oc.add(new InputClause(stream, null, partition_fields, null));
        return new CreateSourceOrTargetStmt(EntityType.TARGET, n, r, dest, formatter, oc);
    }
    
    public static Stmt CreateSubscriptionStatement(final String n, final Boolean r, final AdapterDescription dest, final AdapterDescription formatter, final String stream, final List<String> partition_fields) {
        AdapterDescription temp = null;
        final Property p = new Property("isSubscription", "true");
        if (dest.getProps() == null) {
            final List<Property> list = new ArrayList<Property>();
            list.add(p);
            temp = new AdapterDescription(dest.getAdapterTypeName(), list);
        }
        else {
            dest.getProps().add(p);
            temp = new AdapterDescription(dest.getAdapterTypeName(), dest.getProps());
        }
        final List<InputOutputSink> oc = new ArrayList<InputOutputSink>();
        oc.add(new OutputClause(stream, null, partition_fields, null, null, null));
        return new CreateSourceOrTargetStmt(EntityType.TARGET, n, r, temp, formatter, oc);
    }
    
    public static Stmt CreateCacheStatement(final String n, final Boolean r, final AdapterDescription src, final AdapterDescription parser, final List<Property> query_props, final String typename) {
        return new CreateCacheStmt(EntityType.CACHE, n, r, src, parser, query_props, typename);
    }
    
    public static Stmt CreateFlowStatement(final String n, final Boolean r, final EntityType type, final List<Pair<EntityType, String>> l, final Boolean encrypt, final RecoveryDescription recov, final ExceptionHandler eh) {
        return new CreateAppOrFlowStatement(type, n, r, l, encrypt, recov, eh, null, null);
    }
    
    public static Stmt CreateDeploymentGroupStatement(final String groupname, final List<String> deploymentGroup, final Long minServers, final Long maxApps) {
        return new CreateDeploymentGroupStmt(groupname, deploymentGroup, minServers, maxApps);
    }
    
    public static Stmt CreatePropertySet(final String n, final Boolean r, final List<Property> props) {
        return new CreatePropertySetStmt(n, r, props);
    }
    
    public static Stmt CreatePropertyVariable(final String paramname, final Object paramvalue, final Boolean r) {
        final Serializable s = (paramvalue instanceof Serializable || paramvalue == null) ? ((Serializable)paramvalue) : paramvalue.toString();
        return new CreatePropertyVariableStmt(paramname, s, r);
    }
    
    public static Stmt CreateStartStmt(final String appOrFlowName, final EntityType type, final RecoveryDescription recov) {
        return new ActionStmt(ActionType.START, appOrFlowName, type, recov);
    }
    
    public static Stmt CreateStopStmt(final String appOrFlowName, final EntityType type) {
        return new ActionStmt(ActionType.STOP, appOrFlowName, type);
    }
    
    public static Stmt CreateQuiesceStmt(final String appOrFlowName, final EntityType type) {
        return new ActionStmt(ActionType.QUIESCE, appOrFlowName, type);
    }
    
    public static Stmt CreateResumeStmt(final String appOrFlowName, final EntityType type) {
        return new ActionStmt(ActionType.RESUME, appOrFlowName, type);
    }
    
    public static Stmt CreateWASStatement(final String n, final Boolean r, final TypeDefOrName def, final List<EventType> ets, final HStorePersistencePolicy hStorePersistencePolicy) {
        return new CreateWASStmt(n, r, def, ets, hStorePersistencePolicy.howOften, hStorePersistencePolicy.properties);
    }
    
    public static EventType CreateEventType(final String tpname, final List<String> key) {
        return new EventType(tpname, key);
    }
    
    public static Stmt CreateNamespaceStatement(final String n, final Boolean r) {
        return new CreateNamespaceStatement(n, r);
    }
    
    public static Stmt CreateAdHocSelectStmt(final Select select, final String selectText, final String queryName) {
        return new CreateAdHocSelectStmt(select, selectText, queryName);
    }
    
    public static Stmt CreateAdHocSelectStmt(final Select select, final Grammar p, final String queryName) {
        final String selectText = getExprText(select, p);
        return CreateAdHocSelectStmt(select, selectText, queryName);
    }
    
    public static Stmt CreateShowStmt(final String stream_name) {
        return new CreateShowStreamStmt(stream_name);
    }
    
    public static Stmt CreateShowStmt(final String stream_name, final int line_count) {
        return new CreateShowStreamStmt(stream_name, line_count);
    }
    
    public static Stmt CreateShowStmt(final String stream_name, final int line_count, final boolean isTungsten) {
        return new CreateShowStreamStmt(stream_name, line_count, isTungsten);
    }
    
    public static Stmt CreateShowStmt(final String component_name, final int limit, final List<String> props, final boolean isDescending) {
        return new CreateShowSourceOrTargetStmt(component_name, limit, props, isDescending);
    }
    
    public static Stmt CreateStatusStmt(final String appName) {
        return new ActionStmt(ActionType.STATUS, appName, EntityType.APPLICATION);
    }
    
    public static Stmt CreateMemoryStatusStmt(final String appName) {
        return new ActionStmt(ActionType.MEMORY_STATUS, appName, EntityType.APPLICATION);
    }
    
    public static Stmt CreateUserStatement(final String userid, final String password, final UserProperty prop, final String authLayerPropSetName, final String alias) {
        return new CreateUserStmt(userid, password, prop, authLayerPropSetName, alias);
    }
    
    public static Stmt CreateRoleStatement(final String name) {
        return new CreateRoleStmt(name);
    }
    
    public static Stmt RevokePermissionFromRole(@NotNull final List<ObjectPermission.Action> listOfPrivilege, @Nullable final List<ObjectPermission.ObjectType> objectType, @NotNull final String name, @Nullable final String roleName) {
        return new RevokePermissionFromStmt(listOfPrivilege, objectType, name, roleName, EntityType.ROLE);
    }
    
    public static Stmt RevokePermissionFromUser(@NotNull final List<ObjectPermission.Action> listOfPrivilege, @Nullable final List<ObjectPermission.ObjectType> objectType, @NotNull final String name, @Nullable final String userName) {
        return new RevokePermissionFromStmt(listOfPrivilege, objectType, name, userName, EntityType.USER);
    }
    
    public Stmt RevokeRoleFromRole(final List<String> rolename, final String rolename_2) {
        return new RevokeRoleFromStmt(rolename, rolename_2, EntityType.ROLE);
    }
    
    public Stmt RevokeRoleFromUser(final List<String> rolename, final String username) {
        return new RevokeRoleFromStmt(rolename, username, EntityType.USER);
    }
    
    public static Stmt GrantPermissionToRole(@NotNull final List<ObjectPermission.Action> listOfPrivilege, @Nullable final List<ObjectPermission.ObjectType> objectType, @NotNull final String name, @Nullable final String roleName) {
        return new GrantPermissionToStmt(listOfPrivilege, objectType, name, roleName, EntityType.ROLE);
    }
    
    public static Stmt GrantPermissionToUser(@NotNull final List<ObjectPermission.Action> listOfPrivilege, @Nullable final List<ObjectPermission.ObjectType> objectType, @NotNull final String name, @Nullable final String userName) {
        return new GrantPermissionToStmt(listOfPrivilege, objectType, name, userName, EntityType.USER);
    }
    
    public Stmt GrantRoleToRole(final List<String> rolename, final String rolename_2) {
        return new GrantRoleToStmt(rolename, rolename_2, EntityType.ROLE);
    }
    
    public Stmt GrantRoleToUser(final List<String> rolename, final String username) {
        return new GrantRoleToStmt(rolename, username, EntityType.USER);
    }
    
    public static Stmt CreateLoadUnloadJarStmt(final String pathToJar, final boolean doLoad) {
        return new LoadUnloadJarStmt(pathToJar, doLoad);
    }
    
    public static Stmt Connect(final String username) {
        return new ConnectStmt(username, null, null, null);
    }
    
    public static Stmt ImportData(final String what, final String where, final Boolean replace) {
        return new ImportDataStmt(what, where, replace);
    }
    
    public static Stmt ExportData(final String what) {
        return ExportData(what, null);
    }
    
    public static Stmt ExportData(final String what, final String where) {
        return new ExportDataStmt(what, where);
    }
    
    public static Stmt ExportTypes(final String appname, final String jarpath, final String format) {
        return new ExportAppStmt(appname, jarpath, format);
    }
    
    public static Stmt Connect(final String username, final String password) {
        return new ConnectStmt(username, password, null, null);
    }
    
    public static Stmt Set(final String paramname, final Object paramvalue) {
        final Serializable s = (paramvalue instanceof Serializable || paramvalue == null) ? ((Serializable)paramvalue) : paramvalue.toString();
        return new SetStmt(paramname, s);
    }
    
    public static Stmt endStmt(final List<Property> compileOptions, final Stmt s, final Grammar p) {
        final String stmtText = getExprText(s, p);
        s.setSourceText(stmtText, compileOptions);
        return s;
    }
    
    public static OrderByItem CreateOrderByItem(final ValueExpr expr, final boolean isAscending) {
        return new OrderByItem(expr, isAscending);
    }
    
    public static LimitClause CreateLimitClause(final int limit, final int offset) {
        return new LimitClause(limit, offset);
    }
    
    public static AdapterDescription CreateAdapterDesc(final String type, final List<Property> props) {
        return CreateAdapterDesc(type, null, props);
    }
    
    public static AdapterDescription CreateAdapterDesc(final String type, final String version, final List<Property> props) {
        return new AdapterDescription(type, version, props);
    }
    
    public static RecoveryDescription CreateRecoveryDesc(final int type, final long interval) {
        return new RecoveryDescription(type, interval);
    }
    
    public static ExceptionHandler CreateExceptionHandler(final List<Property> props) {
        return new ExceptionHandler(props);
    }
    
    public static Stmt emptyStmt() {
        return new EmptyStmt();
    }
    
    public static Stmt CreateVisualization(final String objectName, final String filename) {
        return new CreateVisualizationStmt(objectName, filename);
    }
    
    public static Stmt UpdateUserInfoStmt(final String userid, final List<Property> props) {
        return new UpdateUserInfoStmt(userid, props);
    }
    
    public static DataSource ImplicitWindowOverStreamOrHDStoreView(final String streamOrHDStoreName, final String alias, final boolean isJumping, final Pair<IntervalPolicy, IntervalPolicy> wind, final List<String> part) {
        return new DataSourceImplicitWindowOrHDStoreView(streamOrHDStoreName, alias, wind, isJumping, part);
    }
    
    public static DataSource HDStoreView(final String hdStoreName, final String alias, final Boolean isJumping, final Interval range, final boolean subscribeToUpdates) {
        return new DataSourceHDStoreView(hdStoreName, alias, isJumping, range, subscribeToUpdates);
    }
    
    public static DataSource SourceNestedCollection(final String fieldName, final String alias) {
        return new DataSourceNestedCollection(fieldName, alias, null, null);
    }
    
    public static DataSource SourceNestedCollectionOfType(final String fieldName, final String alias, final TypeName type) {
        return new DataSourceNestedCollection(fieldName, alias, type, null);
    }
    
    public static DataSource SourceNestedCollectionWithFields(final String fieldName, final String alias, final List<TypeField> fields) {
        return new DataSourceNestedCollection(fieldName, alias, null, fields);
    }
    
    public static Stmt CreateEndStmt(final String appOrFlowName, final EntityType type) {
        return new EndBlockStmt(appOrFlowName, type);
    }
    
    public static Stmt CreateUndeployStmt(final String appOrFlowName, final EntityType type) {
        return new ActionStmt(ActionType.UNDEPLOY, appOrFlowName, type);
    }
    
    public static Stmt CreateDeployStmt(final EntityType type, final DeploymentRule appRule, final List<DeploymentRule> flowRules, final List<Pair<String, String>> options) {
        return new DeployStmt(type, appRule, flowRules, options);
    }
    
    public static DeploymentRule CreateDeployRule(final DeploymentStrategy ds, final String flowName, final String deploymentgroup) {
        return new DeploymentRule(ds, flowName, deploymentgroup);
    }
    
    public SorterInOutRule CreateSorterInOutRule(final String instream, final String fieldname, final String outstream) {
        return new SorterInOutRule(instream, fieldname, outstream);
    }
    
    public Stmt CreateSorterStatement(final Boolean r, final String n, final Interval i, final List<SorterInOutRule> l, final String errorStream) {
        return new CreateSorterStmt(n, r, i, l, errorStream);
    }
    
    public static GracePeriod CreateGracePeriodClause(final Interval t, final String fieldname) {
        return new GracePeriod(t, fieldname);
    }
    
    public static Predicate BooleanExprPredicate(final ValueExpr e) {
        final ArrayList<ValueExpr> args = new ArrayList<ValueExpr>();
        args.add(e);
        return new ComparePredicate(ExprCmd.BOOLEXPR, args);
    }
    
    public static Stmt ExecPreparedQuery(final String queryName, List<Property> params) {
        if (params == null) {
            params = Collections.emptyList();
        }
        return new ExecPreparedStmt(queryName, params);
    }
    
    private static List<String> extractPermissionsFromActionEntityTypePair(final List<Pair<ObjectPermission.Action, EntityType>> pair, final String optional_domain) {
        final List<String> permissionList = new ArrayList<String>();
        for (final Pair<ObjectPermission.Action, EntityType> pairEntry : pair) {
            final StringBuilder code = new StringBuilder();
            if (optional_domain == null) {
                code.append("*:");
            }
            else {
                code.append(optional_domain + ":");
            }
            code.append(pairEntry.first);
            code.append(":");
            code.append(pairEntry.second.toString().toLowerCase());
            code.append(":*");
            permissionList.add(code.toString());
        }
        return permissionList;
    }
    
    private static List<String> extractPermissionListFromActionsList(final List<ObjectPermission.Action> actionsList, final String objectName) {
        final List<String> permissionList = new ArrayList<String>();
        String domain = Utility.splitDomain(objectName);
        final String name = Utility.splitName(objectName);
        MetaInfo.MetaObject mo = null;
        for (final EntityType et : EntityType.values()) {
            try {
                if (et.isGlobal()) {
                    domain = "Global";
                }
                if (name != null && domain != null && et != null) {
                    mo = MDClientOps.getINSTANCE().getMetaObjectByName(et, domain, name, null, HSecurityManager.TOKEN);
                    if (mo != null) {
                        break;
                    }
                }
            }
            catch (MetaDataRepositoryException ex) {}
        }
        if (mo == null) {
            throw new FatalError("Coudn't find metaobject " + objectName);
        }
        for (final ObjectPermission.Action action : actionsList) {
            final StringBuilder code = new StringBuilder();
            code.append(domain + ":");
            code.append(action.toString().toLowerCase() + ":");
            code.append(mo.type.name().toLowerCase() + ":");
            code.append(name);
            permissionList.add(code.toString());
        }
        return permissionList;
    }
    
    public static Stmt CreateAlterDeploymentGroup(final String groupname, final List<String> nodesAdded, final List<String> nodesRemoved, final Long minServers, final Long limitApplications) {
        return new AlterDeploymentGroupStmt(groupname, nodesAdded, nodesRemoved, minServers, limitApplications);
    }
    
    public static Stmt CreateWaitStmt(final int millisec) {
        return new Stmt() {
            @Override
            public Object compile(final Compiler c) throws MetaDataRepositoryException {
                try {
                    Thread.sleep(millisec);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }
    
    public static Stmt CreateDashboardStatement(final Boolean doReplace, final String filename) {
        return new CreateDashboardStatement(doReplace, filename);
    }
    
    public Select CreateSelectMatch(final Boolean distinct, final Integer kind, final List<SelectTarget> target, final List<DataSource> from, final PatternNode pattern, final List<PatternDefinition> definitions, final List<ValueExpr> partitionkey) {
        return new Select(distinct, kind, target, from, null, null, null, null, null, false, new MatchClause(pattern, definitions, partitionkey));
    }
    
    public PatternNode NewPatternAlternation(final PatternNode a, final PatternNode b) {
        return PatternNode.makeComplexNode(ComplexNodeType.ALTERNATION, a, b);
    }
    
    public PatternNode NewPatternCombination(final PatternNode a, final PatternNode b) {
        return PatternNode.makeComplexNode(ComplexNodeType.COMBINATION, a, b);
    }
    
    public PatternNode NewPatternSequence(final PatternNode a, final PatternNode b) {
        return PatternNode.makeComplexNode(ComplexNodeType.SEQUENCE, a, b);
    }
    
    public static PatternDefinition NewPatternDefinition(final String var, final String streamName, final Predicate p) {
        return new PatternDefinition(var, streamName, p);
    }
    
    public PatternNode NewPatternElement(final String name) {
        return PatternNode.makeVariable(name);
    }
    
    public PatternNode NewPatternRepetition(final PatternNode p, final PatternRepetition r) {
        return PatternNode.makeRepetition(p, r);
    }
    
    public PatternNode NewPatternRestartAnchor() {
        return PatternNode.makeRestartAnchor();
    }
    
    public PatternRepetition PatternRepetition(final int mintimes, final int maxtimes) {
        return new PatternRepetition(mintimes, maxtimes, 0);
    }
    
    public static Stmt MonitorStatement(final List<String> param_list) {
        return new MonitorStmt(param_list);
    }
    
    public Stmt printMetaData(final String mode, final String type, final String command) throws MetaDataRepositoryException {
        return new PrintMetaDataStmt(mode, type, command);
    }
    
    public static Stmt loadFile(final String fname) {
        return new LoadFileStmt(fname.trim());
    }
    
    public static Stmt QuitStmt() {
        return new QuitStmt();
    }
    
    public Stmt GrantStatement(@NotNull final List<ObjectPermission.Action> listOfPrivilege, @Nullable final List<ObjectPermission.ObjectType> objectType, @NotNull final String name, @Nullable final String userName, @Nullable final String roleName) {
        final String objName = (userName != null) ? userName : ((roleName != null) ? roleName : null);
        if (objName == null) {
            throw new RuntimeException("Neither role nor user selected");
        }
        final Stmt stmt = (userName != null) ? GrantPermissionToUser(listOfPrivilege, objectType, name, userName) : GrantPermissionToRole(listOfPrivilege, objectType, name, roleName);
        return stmt;
    }
    
    public Stmt RevokeStatement(final List<ObjectPermission.Action> listOfPrivilege, @Nullable final List<ObjectPermission.ObjectType> objectType, final String name, @Nullable final String userName, @Nullable final String roleName) {
        final String objName = (userName != null) ? userName : ((roleName != null) ? roleName : null);
        if (objName == null) {
            throw new RuntimeException("Neither role nor user selected");
        }
        final Stmt stmt = (userName != null) ? RevokePermissionFromUser(listOfPrivilege, objectType, name, userName) : RevokePermissionFromRole(listOfPrivilege, objectType, name, roleName);
        return stmt;
    }
    
    public static Stmt CreateDumpStatement(final String cqname, final Integer dumpmode, final Integer limitVal) {
        return new DumpStmt(cqname, dumpmode, limitVal);
    }
    
    public static Stmt CreateReportStatement(final String cqname, final Integer reportAction, final List<String> param_list) {
        return new ReportStmt(cqname, reportAction, param_list);
    }
    
    public static Stmt CreateAlterStmt(final String objectName, final Boolean enablePartitionBy, final List<String> partitionBy, final Boolean enablePersistence, final StreamPersistencePolicy persistencePolicy) {
        return new AlterStmt(objectName, enablePartitionBy, partitionBy, enablePersistence, persistencePolicy);
    }
    
    public static StreamPersistencePolicy createStreamPersistencePolicy(final String fullyQualifiedNameOfPropSet) {
        return new StreamPersistencePolicy(fullyQualifiedNameOfPropSet);
    }
    
    public ExportStreamSchemaStmt ExportStreamSchema(final String streamname, final String opt_path, final String opt_filename) {
        return new ExportStreamSchemaStmt(streamname, opt_path, opt_filename);
    }
    
    public Stmt Export(final String name, final String schema) {
        if (name.equalsIgnoreCase("metadata")) {
            return ExportData(name);
        }
        return this.ExportStreamSchema(name, null, null);
    }
    
    public Stmt Export(final String name, final String schema, final String where, final String filename) {
        if (name.equalsIgnoreCase("metadata")) {
            return ExportData(name, where);
        }
        return this.ExportStreamSchema(name, where, filename);
    }
}
