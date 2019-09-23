package com.datasphere.runtime.compiler.select;

import com.datasphere.hdstore.constants.*;
import java.util.*;
import com.datasphere.runtime.compiler.stmts.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.core.*;
import java.io.*;
import com.fasterxml.jackson.databind.node.*;
import org.joda.time.format.*;
import com.datasphere.runtime.compiler.visitors.*;
import java.sql.*;
import org.joda.time.*;
import com.datasphere.runtime.compiler.exprs.*;

public class AST2JSON
{
    private final Set<Capability> caps;
    private static final DateTimeFormatter fmt;
    
    public AST2JSON(final Set<Capability> capabilities) {
        this.caps = capabilities;
    }
    
    private void check(final Capability cap) {
        if (!this.caps.contains(cap)) {
            throw new InvalidWASQueryExprException();
        }
    }
    
    public JsonNode transformFilter(final DataSet ds, final Condition filter) {
        try {
            final ObjectNode stmt = JsonNodeFactory.instance.objectNode();
            final ArrayNode select = array();
            select.add(text("*"));
            this.check(Capability.SELECT);
            stmt.set("select", (JsonNode)select);
            final ArrayNode from = array();
            from.add(text(ds.getFullName()));
            this.check(Capability.FROM);
            stmt.set("from", (JsonNode)from);
            if (filter != null) {
                final ArrayNode where = array();
                for (final Condition.CondExpr cx : filter.getCondExprs()) {
                    try {
                        final JsonNode expr = this.genExpr(cx.expr, ExprKind.WHERE);
                        where.add(expr);
                    }
                    catch (InvalidWASQueryExprException ex) {}
                }
                final int size = where.size();
                if (size > 0) {
                    final JsonNode val = (size > 1) ? obj1("and", (JsonNode)where) : where.get(0);
                    this.check(Capability.WHERE);
                    stmt.set("where", val);
                }
            }
            return (JsonNode)stmt;
        }
        catch (InvalidWASQueryExprException e) {
            return null;
        }
    }
    
    public static void addOperation(final JsonNode query, final String oper, final String attrName, final String attrValue) {
        final JsonNode likeNode = makePredicate(oper, text(attrName), text(attrValue));
        final ObjectNode q = (ObjectNode)query;
        final JsonNode where = q.get("where");
        if (where != null) {
            final ArrayNode wand = (ArrayNode)where.get("and");
            if (wand != null) {
                wand.insert(0, likeNode);
            }
            else {
                q.set("where", likeNode);
            }
        }
        else {
            q.set("where", likeNode);
        }
    }
    
    public static void addAttrFilter(final JsonNode query, final String attrName, final String attrValue) {
        addOperation(query, "eq", attrName, attrValue);
    }
    
    public static void addLike(final JsonNode query, final String likeField) {
        addOperation(query, "like", "*any", likeField);
    }
    
    public static void addTimeBounds(final JsonNode query, final long startTime, final long endTime) {
        addTimeBounds(query, "$timestamp", startTime, endTime);
    }
    
    public static void addTimeBounds(final JsonNode query, final String fieldName, final long startTime, final long endTime) {
        final JsonNode start = makePredicate(getCmpOp(ExprCmd.GTEQ), text(fieldName), (JsonNode)num(startTime));
        final JsonNode end = makePredicate(getCmpOp(ExprCmd.LTEQ), text(fieldName), (JsonNode)num(endTime));
        final ObjectNode q = (ObjectNode)query;
        final JsonNode where = q.get("where");
        if (where != null) {
            final ArrayNode wand = (ArrayNode)where.get("and");
            if (wand != null) {
                wand.insert(0, start);
                wand.insert(1, end);
            }
            else {
                final ArrayNode and = array();
                and.add(start);
                and.add(end);
                and.add(where);
                q.set("where", obj1("and", (JsonNode)and));
            }
        }
        else {
            final ArrayNode and2 = array();
            and2.add(start);
            and2.add(end);
            q.set("where", obj1("and", (JsonNode)and2));
        }
    }
    
    public JsonNode transform(final DataSets dataSets, final List<Predicate> predicates, final List<SelectCompiler.Target> targets, final List<ValueExpr> groupBy, final Predicate having, final List<OrderByItem> orderBy) {
        try {
            final ObjectNode stmt = JsonNodeFactory.instance.objectNode();
            this.generateSelect(stmt, targets);
            this.generateFrom(stmt, dataSets);
            this.generateWhere(stmt, predicates);
            this.generateGroupBy(stmt, groupBy);
            this.generateHaving(stmt, having);
            this.generateOrderBy(stmt, orderBy);
            return (JsonNode)stmt;
        }
        catch (InvalidWASQueryExprException e) {
            return null;
        }
    }
    
    public static byte[] serialize(final JsonNode n) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsBytes((Object)n);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException((Throwable)e);
        }
    }
    
    public static JsonNode deserialize(final byte[] image) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readTree(image);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String prettyPrint(final JsonNode n) {
        final ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writerWithDefaultPrettyPrinter().writeValueAsString((Object)n);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException((Throwable)e);
        }
    }
    
    public static Expr skipCastOps(Expr e) {
        while (e instanceof CastOperation) {
            final CastOperation c = (CastOperation)e;
            e = c.args.get(0);
        }
        return e;
    }
    
    private static NumericNode num(final long x) {
        return JsonNodeFactory.instance.numberNode(x);
    }
    
    public static ArrayNode array() {
        return JsonNodeFactory.instance.arrayNode();
    }
    
    public static JsonNode text(final String s) {
        final TextNode n = JsonNodeFactory.instance.textNode(s);
        return (JsonNode)n;
    }
    
    private static JsonNode bool(final boolean b) {
        final BooleanNode n = JsonNodeFactory.instance.booleanNode(b);
        return (JsonNode)n;
    }
    
    public static JsonNode obj1(final String key, final JsonNode value) {
        final ObjectNode ret = JsonNodeFactory.instance.objectNode();
        ret.set(key, value);
        return (JsonNode)ret;
    }
    
    private static JsonNode obj2(final String key1, final JsonNode value1, final String key2, final JsonNode value2) {
        final ObjectNode ret = JsonNodeFactory.instance.objectNode();
        ret.set(key1, value1);
        ret.set(key2, value2);
        return (JsonNode)ret;
    }
    
    public static JsonNode makePredicate(final String oper, final JsonNode attr, final JsonNode val) {
        final ObjectNode pred = JsonNodeFactory.instance.objectNode();
        pred.set("oper", text(oper));
        pred.set("attr", attr);
        if (val.isArray()) {
            final ArrayNode a = (ArrayNode)val;
            if (a.size() > 1) {
                pred.set("values", (JsonNode)a);
            }
            else {
                pred.set("value", a.get(0));
            }
        }
        else {
            pred.set("value", val);
        }
        return (JsonNode)pred;
    }
    
    private JsonNode genExpr(final Expr e, final ExprKind kind) {
        final Expression2JsonTransformer t = new Expression2JsonTransformer(e, kind);
        return t.result;
    }
    
    private void generateSelect(final ObjectNode stmt, final List<SelectCompiler.Target> targets) {
        final ArrayNode select = array();
        for (final SelectCompiler.Target t : targets) {
            final JsonNode expr = this.genExpr(t.expr, ExprKind.PROJECTION);
            select.add(expr);
        }
        this.check(Capability.SELECT);
        stmt.set("select", (JsonNode)select);
    }
    
    private void generateFrom(final ObjectNode stmt, final DataSets dataSets) {
        final ArrayNode from = array();
        for (final DataSet ds : dataSets) {
            final JsonNode val = text(ds.getFullName());
            from.add(val);
        }
        this.check(Capability.FROM);
        stmt.set("from", (JsonNode)from);
    }
    
    private void generateWhere(final ObjectNode stmt, final List<Predicate> predicates) {
        if (!predicates.isEmpty()) {
            final ArrayNode where = array();
            for (final Predicate p : predicates) {
                final JsonNode expr = this.genExpr(p, ExprKind.WHERE);
                where.add(expr);
            }
            final JsonNode val = (where.size() > 1) ? obj1("and", (JsonNode)where) : where.get(0);
            this.check(Capability.WHERE);
            stmt.set("where", val);
        }
    }
    
    private void generateGroupBy(final ObjectNode stmt, final List<ValueExpr> groupBy) {
        if (groupBy != null && !groupBy.isEmpty()) {
            final ArrayNode groupby = array();
            for (final ValueExpr e : groupBy) {
                final JsonNode expr = this.genExpr(e, ExprKind.GROUPBY);
                groupby.add(expr);
            }
            this.check(Capability.GROUP_BY);
            stmt.set("groupby", (JsonNode)groupby);
        }
    }
    
    private void generateHaving(final ObjectNode stmt, final Predicate having) {
        if (having != null) {
            final JsonNode expr = this.genExpr(having, ExprKind.HAVING);
            this.check(Capability.HAVING);
            stmt.set("having", expr);
        }
    }
    
    private void generateOrderBy(final ObjectNode stmt, final List<OrderByItem> orderBy) {
        if (orderBy != null && !orderBy.isEmpty()) {
            final ArrayNode orderby = array();
            for (final OrderByItem e : orderBy) {
                final JsonNode expr = this.genExpr(e.expr, ExprKind.ORDERBY);
                orderby.add(obj2("attr", expr, "ascending", bool(e.isAscending)));
            }
            this.check(Capability.ORDER_BY);
            stmt.set("orderby", (JsonNode)orderby);
        }
    }
    
    private static String getCmpOp(final ExprCmd op) {
        switch (op) {
            case EQ: {
                return "eq";
            }
            case NOTEQ: {
                return "neq";
            }
            case GT: {
                return "gt";
            }
            case GTEQ: {
                return "gte";
            }
            case LT: {
                return "lt";
            }
            case LTEQ: {
                return "lte";
            }
            case ISNULL: {
                return "isnull";
            }
            case LIKE: {
                return "like";
            }
            case INLIST: {
                return "in";
            }
            case BETWEEN: {
                return "between";
            }
            default: {
                assert false;
                return null;
            }
        }
    }
    
    private static String getOp(final ExprCmd op) {
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
    
    static {
        fmt = new DateTimeFormatterBuilder().append(ISODateTimeFormat.date()).appendLiteral(' ').append(ISODateTimeFormat.time()).toFormatter();
    }
    
    private enum ExprKind
    {
        PROJECTION, 
        WHERE, 
        HAVING, 
        GROUPBY, 
        ORDERBY;
    }
    
    private static class Jexpr
    {
        final ArrayNode args;
        JsonNode value;
        
        private Jexpr() {
            this.args = AST2JSON.array();
        }
        
        @Override
        public String toString() {
            return "JEXPR(value:" + this.value + ", args:" + this.args + ")";
        }
    }
    
    private class Expression2JsonTransformer extends ExpressionVisitorDefaultImpl<Jexpr>
    {
        private final ExprKind kind;
        JsonNode result;
        
        public Expression2JsonTransformer(final Expr e, final ExprKind kind) {
            if (e instanceof DataSetRef) {
                throw new InvalidWASQueryExprException();
            }
            if (e instanceof Constant) {
                throw new InvalidWASQueryExprException();
            }
            this.kind = kind;
            final Jexpr ret = new Jexpr();
            this.visitExpr(e, ret);
            this.result = ret.args.get(0);
        }
        
        @Override
        public Expr visitExpr(final Expr e, final Jexpr ret) {
            final Jexpr var = new Jexpr();
            e.visitArgs(this, var);
            e.visit(this, var);
            ret.args.add(var.value);
            return null;
        }
        
        @Override
        public Expr visitExprDefault(final Expr e, final Jexpr ret) {
            throw new InvalidWASQueryExprException();
        }
        
        @Override
        public Expr visitNumericOperation(final NumericOperation operation, final Jexpr ret) {
            throw new InvalidWASQueryExprException();
        }
        
        @Override
        public Expr visitConstant(final Constant constant, final Jexpr ret) {
            assert ret.args.size() == 0;
            final Object cval = constant.value;
            JsonNode value = null;
            switch (constant.op) {
                case BOOL:
                case DOUBLE:
                case FLOAT:
                case LONG:
                case INT: {
                    value = AST2JSON.text(cval.toString());
                    break;
                }
                case INTERVAL: {
                    value = obj2("unit", AST2JSON.text("seconds"), "amount", AST2JSON.text("" + (long)cval / 1000000L));
                    break;
                }
                case NULL: {
                    value = (JsonNode)JsonNodeFactory.instance.nullNode();
                    break;
                }
                case STRING: {
                    value = AST2JSON.text((String)cval);
                    break;
                }
                case TIMESTAMP: {
                    final Timestamp t = (Timestamp)cval;
                    final DateTime d = new DateTime(t.getTime());
                    value = AST2JSON.text(d.toString(AST2JSON.fmt));
                    break;
                }
                case DATETIME: {
                    final DateTime dt = (DateTime)cval;
                    value = AST2JSON.text(dt.toString(AST2JSON.fmt));
                    break;
                }
                default: {
                    assert false;
                    break;
                }
            }
            ret.value = value;
            return null;
        }
        
        @Override
        public Expr visitCase(final CaseExpr caseExpr, final Jexpr ret) {
            throw new InvalidWASQueryExprException();
        }
        
        private JsonNode transformAggFuncName(String name) {
            final String lowerCase;
            name = (lowerCase = name.toLowerCase());
            switch (lowerCase) {
                case "avg": {
                    AST2JSON.this.check(Capability.AGGREGATION_AVG);
                    break;
                }
                case "count": {
                    AST2JSON.this.check(Capability.AGGREGATION_COUNT);
                    break;
                }
                case "sum": {
                    AST2JSON.this.check(Capability.AGGREGATION_SUM);
                    break;
                }
                case "min": {
                    AST2JSON.this.check(Capability.AGGREGATION_MIN);
                    break;
                }
                case "max": {
                    AST2JSON.this.check(Capability.AGGREGATION_MAX);
                    break;
                }
                case "first": {
                    AST2JSON.this.check(Capability.AGGREGATION_FIRST);
                    break;
                }
                case "last": {
                    AST2JSON.this.check(Capability.AGGREGATION_LAST);
                    break;
                }
                default: {
                    throw new InvalidWASQueryExprException();
                }
            }
            return AST2JSON.text(name);
        }
        
        @Override
        public Expr visitFuncCall(final FuncCall funcCall, final Jexpr ret) {
            final String funcName = funcCall.getName();
            if (this.kind == ExprKind.ORDERBY) {
                throw new InvalidWASQueryExprException();
            }
            if (funcCall.getAggrDesc() != null) {
                JsonNode arg;
                if (ret.args.size() == 0) {
                    assert funcName.equalsIgnoreCase("count");
                    arg = AST2JSON.text("$id");
                }
                else {
                    assert ret.args.size() == 1;
                    arg = ret.args.get(0);
                }
                if (funcCall.isDistinct()) {
                    throw new InvalidWASQueryExprException();
                }
                ret.value = obj2("oper", this.transformAggFuncName(funcName), "attr", arg);
            }
            else if (funcCall.isCustomFunc() != null && funcName.equalsIgnoreCase("anyattrlike")) {
                AST2JSON.this.check(Capability.LIKE);
                AST2JSON.this.check(Capability.DATA_ANY);
                if (!this.checkConst(funcCall.getFuncArgs().get(1))) {
                    throw new InvalidWASQueryExprException();
                }
                ret.value = AST2JSON.makePredicate("like", AST2JSON.text("*any"), ret.args.get(1));
            }
            else {
                if (!funcName.equalsIgnoreCase("compile__like__pattern")) {
                    throw new InvalidWASQueryExprException();
                }
                ret.value = ret.args.get(0);
            }
            return null;
        }
        
        @Override
        public Expr visitFieldRef(final FieldRef fieldRef, final Jexpr ret) {
            ret.value = AST2JSON.text(fieldRef.getName());
            return null;
        }
        
        @Override
        public Expr visitLogicalPredicate(final LogicalPredicate logicalPredicate, final Jexpr ret) {
            JsonNode value = null;
            switch (logicalPredicate.op) {
                case AND: {
                    value = AST2JSON.obj1("and", (JsonNode)ret.args);
                    break;
                }
                case OR: {
                    value = AST2JSON.obj1("or", (JsonNode)ret.args);
                    break;
                }
                case NOT: {
                    value = AST2JSON.obj1("not", ret.args.get(0));
                    break;
                }
                default: {
                    assert false;
                    break;
                }
            }
            ret.value = value;
            return null;
        }
        
        private boolean checkAttr(final Expr e) {
            return AST2JSON.skipCastOps(e) instanceof FieldRef;
        }
        
        private boolean checkConst(final Expr e) {
            return AST2JSON.skipCastOps(e) instanceof Constant;
        }
        
        private boolean validateCond(final ComparePredicate p) {
            if (p.args.size() < 2) {
                return false;
            }
            final Iterator<ValueExpr> args = p.args.iterator();
            if (!this.checkAttr(args.next())) {
                return false;
            }
            while (args.hasNext()) {
                if (!this.checkConst(args.next())) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        public Expr visitComparePredicate(final ComparePredicate p, final Jexpr ret) {
            switch (p.op) {
                case BOOLEXPR: {
                    throw new InvalidWASQueryExprException();
                }
                case ISNULL:
                case CHECKRECTYPE: {
                    throw new InvalidWASQueryExprException();
                }
                case EQ:
                case NOTEQ:
                case GT:
                case GTEQ:
                case LT:
                case LTEQ:
                case LIKE:
                case INLIST:
                case BETWEEN: {
                    if (!this.validateCond(p)) {
                        throw new InvalidWASQueryExprException();
                    }
                    break;
                }
                default: {
                    assert false;
                    break;
                }
            }
            final ArrayNode args = ret.args;
            final JsonNode attr = args.remove(0);
            ret.value = AST2JSON.makePredicate(getCmpOp(p.op), attr, (JsonNode)args);
            return null;
        }
        
        @Override
        public Expr visitDataSetRef(final DataSetRef dataSetRef, final Jexpr ret) {
            ret.value = AST2JSON.text(dataSetRef.getDataSet().getName());
            return null;
        }
        
        @Override
        public Expr visitCastExpr(final CastExpr castExpr, final Jexpr ret) {
            final JsonNode value = ret.args.get(0);
            ret.value = value;
            return null;
        }
        
        @Override
        public Expr visitCastOperation(final CastOperation castOp, final Jexpr ret) {
            final JsonNode value = ret.args.get(0);
            ret.value = value;
            return null;
        }
    }
    
    private static class InvalidWASQueryExprException extends RuntimeException
    {
        private static final long serialVersionUID = -5463707233439365065L;
    }
}
