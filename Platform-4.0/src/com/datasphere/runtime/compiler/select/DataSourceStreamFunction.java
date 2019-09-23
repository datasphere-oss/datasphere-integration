package com.datasphere.runtime.compiler.select;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import com.datasphere.classloading.WALoader;
import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.BuiltInFunc;
import com.datasphere.runtime.Context;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.runtime.compiler.custom.StreamGeneratorDef;
import com.datasphere.runtime.compiler.exprs.Constant;
import com.datasphere.runtime.compiler.exprs.ValueExpr;
import com.datasphere.runtime.meta.MetaInfo;
import com.datasphere.runtime.utils.RuntimeUtils;
import com.datasphere.runtime.utils.StringUtils;

public class DataSourceStreamFunction extends DataSourcePrimary
{
    private final String funcName;
    private final List<ValueExpr> args;
    private final String alias;
    
    public DataSourceStreamFunction(final String funcName, final List<ValueExpr> args, final String alias) {
        this.funcName = funcName;
        this.args = args;
        this.alias = alias;
    }
    
    @Override
    public String toString() {
        return this.funcName + "(" + StringUtils.join((List)this.args) + ") AS " + this.alias;
    }
    
    @Override
    public DataSet resolveDataSourceImpl(final DataSource.Resolver r) throws MetaDataRepositoryException {
        final Compiler co = r.getCompiler();
        final Context ctx = co.getContext();
        final String name = this.funcName;
        try {
            final int index = name.lastIndexOf(46);
            Class<?> klass;
            String methodName;
            if (index == -1) {
                klass = BuiltInFunc.class;
                methodName = name;
            }
            else {
                final String className = name.substring(0, index);
                methodName = name.substring(index + 1);
                klass = ctx.getClass(className);
            }
            int i = 1;
            final Object[] params = new Object[this.args.size() + 1];
            final Class<?>[] ptypes = (Class<?>[])new Class[this.args.size() + 1];
            params[0] = WALoader.get();
            ptypes[0] = ClassLoader.class;
            for (final ValueExpr expr : this.args) {
                if (!(expr instanceof Constant)) {
                    r.error("stream function parameter must be a constant", expr);
                }
                else {
                    final Constant constant = (Constant)expr;
                    params[i] = constant.value;
                    ptypes[i] = constant.getType();
                    ++i;
                }
            }
            final Method meth = klass.getMethod(methodName, ptypes);
            final Object o = meth.invoke(null, params);
            final StreamGeneratorDef gdef = (StreamGeneratorDef)o;
            final MetaInfo.Type t = co.createAnonType(gdef.outputTypeID, gdef.outputType);
            ctx.putTmpObject(t);
            final MetaInfo.StreamGenerator g = new MetaInfo.StreamGenerator();
            g.construct(RuntimeUtils.genRandomName("generator"), ctx.getCurNamespace(), gdef.outputTypeID, gdef.generatorClassName, gdef.args);
            g.getMetaInfoStatus().setAnonymous(true);
            ctx.putTmpObject(g);
            r.addTypeID(gdef.outputTypeID);
            final DataSet ds = DataSetFactory.createStreamFunctionDS(r.getNextDataSetID(), this.alias, gdef.outputType, g);
            return ds;
        }
        catch (ClassNotFoundException e) {
            r.error("cannot load stream generator function: no such class", name);
        }
        catch (NoSuchMethodException e2) {
            r.error("cannot load stream generator function: no such method", name);
        }
        catch (SecurityException e3) {
            r.error("cannot load stream generator function: method inaccessible", name);
        }
        catch (IllegalAccessException e4) {
            r.error("cannot load stream generator function: method inaccessible", name);
        }
        catch (IllegalArgumentException e5) {
            r.error("cannot load stream generator function: illegal arguments", name);
        }
        catch (InvocationTargetException e6) {
            r.error("cannot load stream generator function: exception in function", name);
        }
        return null;
    }
}
