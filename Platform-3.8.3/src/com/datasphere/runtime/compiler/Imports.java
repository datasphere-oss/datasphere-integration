package com.datasphere.runtime.compiler;

import java.lang.reflect.*;
import com.datasphere.runtime.utils.*;
import java.util.*;

public class Imports
{
    private final Map<String, Field> staticVars;
    private final Map<String, Class<?>> classes;
    private final Map<String, List<Class<?>>> staticMethods;
    
    public Imports() {
        this.staticVars = Factory.makeNameMap();
        this.classes = Factory.makeNameMap();
        this.staticMethods = Factory.makeNameMap();
    }
    
    public Class<?> findClass(final String name) {
        return this.classes.get(name);
    }
    
    public void importPackage(final String pkgname) {
        assert false : "not implemented yet";
    }
    
    public Class<?> importClass(final Class<?> c) {
        final Class<?> pc = this.classes.get(c.getSimpleName());
        if (pc == null || pc.equals(c)) {
            this.classes.put(c.getSimpleName(), c);
            return null;
        }
        return c;
    }
    
    public Field getStaticFieldRef(final String name) {
        return this.staticVars.get(name);
    }
    
    public Field addStaticFieldRef(final Field f) {
        final Field pf = this.staticVars.get(f.getName());
        if (pf == null || pf.equals(f)) {
            this.staticVars.put(f.getName(), f);
            return null;
        }
        return pf;
    }
    
    public List<Class<?>> getStaticMethodRef(final String name, final List<Class<?>> firstLookHere) {
        final List<Class<?>> list = this.staticMethods.get(name);
        if (list != null) {
            final List<Class<?>> ret = new ArrayList<Class<?>>(firstLookHere);
            ret.addAll(list);
            return ret;
        }
        return firstLookHere;
    }
    
    public List<Class<?>> getStaticMethodRef(final String name, final Class<?>... firstLookHere) {
        return this.getStaticMethodRef(name, Arrays.asList(firstLookHere));
    }
    
    public void addStaticMethod(final Class<?> c, final String name) {
        List<Class<?>> l = this.staticMethods.get(name);
        if (l == null) {
            l = new LinkedList<Class<?>>();
            l.add(c);
            this.staticMethods.put(name, l);
        }
        else if (!l.contains(c)) {
            l.add(c);
        }
    }
}
