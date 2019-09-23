package com.datasphere.runtime.compiler.custom;

import com.datasphere.runtime.compiler.*;
import java.lang.reflect.*;
import java.util.*;
import com.datasphere.runtime.compiler.exprs.*;

public class BuiltinNumOps
{
    public static ValueExpr rewriteNumOp(final ExprCmd op, final Class<?> operandType, final List<ValueExpr> args) {
        final Method m = findMethod(op, operandType, args.size());
        if (args.size() > 1) {
            ValueExpr ret = null;
            for (final ValueExpr arg : args) {
                if (ret == null) {
                    ret = arg;
                }
                else {
                    ret = makeFuncCall(m, AST.NewList(ret, arg));
                }
            }
            return ret;
        }
        assert args.size() == 1;
        return makeFuncCall(m, args);
    }
    
    private static String type2prefix(final Class<?> retType) {
        if (retType == Integer.class) {
            return "i";
        }
        if (retType == Long.class) {
            return "l";
        }
        if (retType == Float.class) {
            return "f";
        }
        if (retType == Double.class) {
            return "d";
        }
        assert false;
        return null;
    }
    
    private static Method findMethod(final ExprCmd op, final Class<?> type, final int nparam) {
        final String opname = type2prefix(type) + op.name().toLowerCase();
        try {
            if (nparam == 1) {
                return BuiltinNumOps.class.getMethod(opname, type);
            }
            return BuiltinNumOps.class.getMethod(opname, type, type);
        }
        catch (NoSuchMethodException | SecurityException ex2) {
            ex2.printStackTrace();
            assert false;
            return null;
        }
    }
    
    private static FuncCall makeFuncCall(final Method m, final List<ValueExpr> fargs) {
        final FuncCall fc = new FuncCall(m.getName(), fargs, 0);
        fc.setMethodRef(m);
        return fc;
    }
    
    public static Integer iplus(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x + y) : null;
    }
    
    public static Integer iminus(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x - y) : null;
    }
    
    public static Integer imul(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x * y) : null;
    }
    
    public static Integer idiv(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x / y) : null;
    }
    
    public static Integer imod(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x % y) : null;
    }
    
    public static Integer ibitand(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x & y) : null;
    }
    
    public static Integer ibitor(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x | y) : null;
    }
    
    public static Integer ibitxor(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x ^ y) : null;
    }
    
    public static Integer ilshift(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x << y) : null;
    }
    
    public static Integer irshift(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x >> y) : null;
    }
    
    public static Integer iurshift(final Integer x, final Integer y) {
        return (x != null && y != null) ? (x >>> y) : null;
    }
    
    public static Integer iuminus(final Integer x) {
        return (x != null) ? (-x) : null;
    }
    
    public static Integer iinvert(final Integer x) {
        return (x != null) ? (~x) : null;
    }
    
    public static Integer iuplus(final Integer x) {
        return x;
    }
    
    public static Long lplus(final Long x, final Long y) {
        return (x != null && y != null) ? (x + y) : null;
    }
    
    public static Long lminus(final Long x, final Long y) {
        return (x != null && y != null) ? (x - y) : null;
    }
    
    public static Long lmul(final Long x, final Long y) {
        return (x != null && y != null) ? (x * y) : null;
    }
    
    public static Long ldiv(final Long x, final Long y) {
        return (x != null && y != null) ? (x / y) : null;
    }
    
    public static Long lmod(final Long x, final Long y) {
        return (x != null && y != null) ? (x % y) : null;
    }
    
    public static Long lbitand(final Long x, final Long y) {
        return (x != null && y != null) ? (x & y) : null;
    }
    
    public static Long lbitor(final Long x, final Long y) {
        return (x != null && y != null) ? (x | y) : null;
    }
    
    public static Long lbitxor(final Long x, final Long y) {
        return (x != null && y != null) ? (x ^ y) : null;
    }
    
    public static Long llshift(final Long x, final Long y) {
        return (x != null && y != null) ? (x << (int)(long)y) : null;
    }
    
    public static Long lrshift(final Long x, final Long y) {
        return (x != null && y != null) ? (x >> (int)(long)y) : null;
    }
    
    public static Long lurshift(final Long x, final Long y) {
        return (x != null && y != null) ? (x >>> (int)(long)y) : null;
    }
    
    public static Long luminus(final Long x) {
        return (x != null) ? (-x) : null;
    }
    
    public static Long linvert(final Long x) {
        return (x != null) ? (~x) : null;
    }
    
    public static Long luplus(final Long x) {
        return x;
    }
    
    public static Float fplus(final Float x, final Float y) {
        return (x != null && y != null) ? (x + y) : null;
    }
    
    public static Float fminus(final Float x, final Float y) {
        return (x != null && y != null) ? (x - y) : null;
    }
    
    public static Float fmul(final Float x, final Float y) {
        return (x != null && y != null) ? (x * y) : null;
    }
    
    public static Float fdiv(final Float x, final Float y) {
        return (x != null && y != null) ? (x / y) : null;
    }
    
    public static Float fmod(final Float x, final Float y) {
        return (x != null && y != null) ? (x % y) : null;
    }
    
    public static Float fuminus(final Float x) {
        return (x != null) ? (-x) : null;
    }
    
    public static Float fuplus(final Float x) {
        return x;
    }
    
    public static Double dplus(final Double x, final Double y) {
        return (x != null && y != null) ? (x + y) : null;
    }
    
    public static Double dminus(final Double x, final Double y) {
        return (x != null && y != null) ? (x - y) : null;
    }
    
    public static Double dmul(final Double x, final Double y) {
        return (x != null && y != null) ? (x * y) : null;
    }
    
    public static Double ddiv(final Double x, final Double y) {
        return (x != null && y != null) ? (x / y) : null;
    }
    
    public static Double dmod(final Double x, final Double y) {
        return (x != null && y != null) ? (x % y) : null;
    }
    
    public static Double duminus(final Double x) {
        return (x != null) ? (-x) : null;
    }
    
    public static Double duplus(final Double x) {
        return x;
    }
}
