package com.datasphere.runtime.window;

import java.util.*;

public class FibonacciHeap
{
    private Node min;
    private int n;
    
    public FibonacciHeap() {
        this.min = null;
        this.n = 0;
    }
    
    private void consolidate() {
        final Node[] A = new Node[45];
        Node start = this.min;
        Node w = this.min;
        do {
            Node x = w;
            Node nextW = w.right;
            int d;
            for (d = x.degree; A[d] != null; ++d) {
                Node y = A[d];
                if (x.key > y.key) {
                    final Node temp = y;
                    y = x;
                    x = temp;
                }
                if (y == start) {
                    start = start.right;
                }
                if (y == nextW) {
                    nextW = nextW.right;
                }
                y.link(x);
                A[d] = null;
            }
            A[d] = x;
            w = nextW;
        } while (w != start);
        this.min = start;
        for (final Node a : A) {
            if (a != null && a.key < this.min.key) {
                this.min = a;
            }
        }
    }
    
    public void decreaseKey(final Node x, final long k) {
        x.key = k;
        final Node y = x.parent;
        if (y != null && k < y.key) {
            y.cut(x, this.min);
            y.cascadingCut(this.min);
        }
        if (k < this.min.key) {
            this.min = x;
        }
    }
    
    public boolean isEmpty() {
        return this.min == null;
    }
    
    public Node insert(final Object x, final long key) {
        final Node node = new Node(x, key);
        if (this.min != null) {
            node.right = this.min;
            node.left = this.min.left;
            this.min.left = node;
            node.left.right = node;
            if (key < this.min.key) {
                this.min = node;
            }
        }
        else {
            this.min = node;
        }
        ++this.n;
        return node;
    }
    
    public Node min() {
        return this.min;
    }
    
    public Node removeMin() {
        final Node z = this.min;
        if (z == null) {
            return null;
        }
        if (z.child != null) {
            z.child.parent = null;
            for (Node x = z.child.right; x != z.child; x = x.right) {
                x.parent = null;
            }
            final Node minleft = this.min.left;
            final Node zchildleft = z.child.left;
            this.min.left = zchildleft;
            zchildleft.right = this.min;
            z.child.left = minleft;
            minleft.right = z.child;
        }
        z.left.right = z.right;
        z.right.left = z.left;
        if (z == z.right) {
            this.min = null;
        }
        else {
            this.min = z.right;
            this.consolidate();
        }
        --this.n;
        return z;
    }
    
    public int size() {
        return this.n;
    }
    
    public static FibonacciHeap union(final FibonacciHeap H1, final FibonacciHeap H2) {
        final FibonacciHeap H3 = new FibonacciHeap();
        if (H1 != null && H2 != null) {
            H3.min = H1.min;
            if (H3.min != null) {
                if (H2.min != null) {
                    H3.min.right.left = H2.min.left;
                    H2.min.left.right = H3.min.right;
                    H3.min.right = H2.min;
                    H2.min.left = H3.min;
                    if (H2.min.key < H1.min.key) {
                        H3.min = H2.min;
                    }
                }
            }
            else {
                H3.min = H2.min;
            }
            H3.n = H1.n + H2.n;
        }
        return H3;
    }
    
    public <T> List<T> toList() {
        final List<T> list = new ArrayList<T>();
        if (this.min == null) {
            return list;
        }
        final Stack<Node> stack = new Stack<Node>();
        stack.push(this.min);
        while (!stack.empty()) {
            Node curr = stack.pop();
            list.add((T)curr.data);
            if (curr.child != null) {
                stack.push(curr.child);
            }
            Node start;
            for (start = curr, curr = curr.right; curr != start; curr = curr.right) {
                list.add((T)curr.data);
                if (curr.child != null) {
                    stack.push(curr.child);
                }
            }
        }
        return list;
    }
    
    @Override
    public String toString() {
        return "FibonacciHeap" + this.toList();
    }
    
    public static class Node
    {
        private Object data;
        private long key;
        private Node parent;
        private Node child;
        private Node right;
        private Node left;
        private int degree;
        private boolean mark;
        
        long getKey() {
            return this.key;
        }
        
        Object getData() {
            return this.data;
        }
        
        public Node(final Object data, final long key) {
            this.data = data;
            this.key = key;
            this.right = this;
            this.left = this;
        }
        
        public void cascadingCut(final Node min) {
            final Node z = this.parent;
            if (z != null) {
                if (this.mark) {
                    z.cut(this, min);
                    z.cascadingCut(min);
                }
                else {
                    this.mark = true;
                }
            }
        }
        
        public void cut(final Node x, final Node min) {
            x.left.right = x.right;
            x.right.left = x.left;
            --this.degree;
            if (this.degree == 0) {
                this.child = null;
            }
            else if (this.child == x) {
                this.child = x.right;
            }
            x.right = min;
            x.left = min.left;
            min.left = x;
            x.left.right = x;
            x.parent = null;
            x.mark = false;
        }
        
        public void link(final Node parent) {
            this.left.right = this.right;
            this.right.left = this.left;
            this.parent = parent;
            if (parent.child == null) {
                parent.child = this;
                this.right = this;
                this.left = this;
            }
            else {
                this.left = parent.child;
                this.right = parent.child.right;
                parent.child.right = this;
                this.right.left = this;
            }
            ++parent.degree;
            this.mark = false;
        }
    }
}
