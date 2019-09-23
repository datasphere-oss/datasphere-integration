package com.datasphere.runtime.compiler.stmts;

import org.apache.log4j.*;
import java.util.*;
import com.datasphere.runtime.compiler.select.*;
import com.datasphere.runtime.compiler.exprs.*;
import java.io.*;

public class Select implements Serializable
{
    private static final Logger logger;
    public final boolean distinct;
    public final int kindOfStream;
    public final List<SelectTarget> targets;
    public List<DataSource> from;
    public final Predicate where;
    public List<ModifyExpr> modify;
    public final List<ValueExpr> groupBy;
    public final Predicate having;
    public final List<OrderByItem> orderBy;
    public final LimitClause limit;
    public final boolean linksrc;
    public final MatchClause match;
    
    public Select(final boolean distinct, final int kind, final List<SelectTarget> targets, final List<DataSource> from, final Predicate where, final List<ValueExpr> groupBy, final Predicate having, final List<OrderByItem> orderBy, final LimitClause limit, final boolean linksrc, final MatchClause match) {
        this.distinct = distinct;
        this.kindOfStream = kind;
        this.targets = targets;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
        this.linksrc = linksrc;
        this.match = match;
    }
    
    @Override
    public String toString() {
        return "SELECT " + (this.distinct ? "DISTINCT " : "") + this.kindOfStream + "\n" + this.targets + "\nFROM " + this.from + "\nWHERE " + this.where + "\nGROUP BY " + this.groupBy + "\nHAVING " + this.having + ((this.match != null) ? ("\n" + this.match) : "");
    }
    
    public Select copyDeep() {
        final Select select = new Select(this.distinct, this.kindOfStream, this.targets, this.from, null, null, null, null, null, false, this.match);
        try {
            final Select copyOfSelect = (Select)copy(select);
            return copyOfSelect;
        }
        catch (Exception e) {
            if (Select.logger.isDebugEnabled()) {
                Select.logger.debug((Object)e.getMessage(), (Throwable)e);
            }
            return this;
        }
    }
    
    public static Object copy(final Object orig) throws Exception {
        Object obj = null;
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(orig);
        out.flush();
        out.close();
        final ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()));
        obj = in.readObject();
        return obj;
    }
    
    static {
        logger = Logger.getLogger(Select.class.getName());
    }
}
