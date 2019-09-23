package com.datasphere.runtime.compiler.select;

import com.datasphere.runtime.compiler.exprs.*;
import java.io.*;
import java.util.*;

public class Join
{
    private static final int LEFT_OUTER_JOIN = 1;
    private static final int RIGHT_OUTER_JOIN = 2;
    
    public static Node createDataSetNode(final DataSet dataset) {
        final LeafNode n = new LeafNode();
        n.dataset = dataset;
        return n;
    }
    
    public static Node createJoinNode(final Node left, final Node right, final Predicate joinCond, final Kind kind) {
        final JoinNode n = new JoinNode();
        n.left = left;
        n.right = right;
        n.joinCondition = joinCond;
        n.kindOfJoin = kind;
        return n;
    }
    
    public static Tree createJoinTree(final Node root) {
        final Tree t = new Tree();
        t.addNodeToJoinList(t.root = root);
        return t;
    }
    
    public enum Kind
    {
        INNER(0), 
        CROSS(0), 
        LEFT(1), 
        RIGHT(2), 
        FULL(3);
        
        private final int flags;
        
        private Kind(final int flags) {
            this.flags = flags;
        }
        
        public boolean isInner() {
            return this.flags == 0;
        }
        
        public boolean isOuter() {
            return this.flags != 0;
        }
        
        public boolean isLeft() {
            return (this.flags & 0x1) > 0;
        }
        
        public boolean isRight() {
            return (this.flags & 0x2) > 0;
        }
    }
    
    public static class LeafNode implements Node
    {
        DataSet dataset;
        
        @Override
        public BitSet getDataSets() {
            return this.dataset.id2bitset();
        }
        
        @Override
        public String toString() {
            return "DS(" + this.dataset.getFullName() + " " + this.dataset.getID() + ")";
        }
    }
    
    public static class JoinNode implements Node
    {
        public Node left;
        public Node right;
        Predicate joinCondition;
        public Kind kindOfJoin;
        
        @Override
        public BitSet getDataSets() {
            final BitSet bs = this.left.getDataSets();
            bs.or(this.right.getDataSets());
            return bs;
        }
        
        @Override
        public String toString() {
            return this.left + " " + this.kindOfJoin + " " + this.right + " ON " + this.joinCondition;
        }
    }
    
    public static class Tree
    {
        Node root;
        LinkedHashMap<Integer, JoinNode> joins;
        
        public Tree() {
            this.joins = new LinkedHashMap<Integer, JoinNode>();
        }
        
        public void print(final PrintStream out) {
            printNode(0, this.root, out);
        }
        
        @Override
        public String toString() {
            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            final PrintStream ps = new PrintStream(os);
            this.print(ps);
            return os.toString();
        }
        
        private static void printNode(int level, final Node n, final PrintStream out) {
            ++level;
            final String ident = new String(new char[level]).replace("\u0000", "--");
            if (n instanceof JoinNode) {
                final JoinNode jn = (JoinNode)n;
                out.println(ident + " " + jn.kindOfJoin + " " + jn.joinCondition);
                printNode(level, jn.left, out);
                printNode(level, jn.right, out);
            }
            else {
                final LeafNode ln = (LeafNode)n;
                out.println(ident + " source:" + ln.dataset);
            }
        }
        
        public JoinNode getJoinPredicate(final Predicate p) {
            return this.joins.get(p.eid);
        }
        
        public void addNodeToJoinList(final Node n) {
            if (n instanceof JoinNode) {
                final JoinNode jn = (JoinNode)n;
                if (jn.joinCondition != null) {
                    this.joins.put(jn.joinCondition.eid, jn);
                }
                this.addNodeToJoinList(jn.left);
                this.addNodeToJoinList(jn.right);
            }
        }
        
        public boolean isOuterJoin(final BitSet alreadyJoined, final int datasetID, final List<Condition.CondExpr> condIndex) {
            boolean outerJoin = false;
            for (final Condition.CondExpr e : condIndex) {
                if (e.isOuterJoinCondtion(alreadyJoined, datasetID)) {
                    outerJoin = true;
                    break;
                }
            }
            if (outerJoin) {
                for (final Condition.CondExpr e : condIndex) {
                    if (e.rejectsNulls(datasetID)) {
                        outerJoin = false;
                    }
                }
            }
            return outerJoin;
        }
    }
    
    public interface Node
    {
        BitSet getDataSets();
    }
}
