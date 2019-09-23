package com.datasphere.runtime.compiler.select;

public class Var
{
    private final String prefix;
    private final int id;
    private final Class<?> type;
    private final boolean isStatic;
    
    public Var(final String prefix, final int id, final Class<?> type, final boolean isStatic) {
        assert type != null;
        this.prefix = prefix;
        this.id = id;
        this.type = type;
        this.isStatic = isStatic;
    }
    
    public Class<?> getType() {
        return this.type;
    }
    
    public String getName() {
        return this.prefix + this.id;
    }
    
    public int getID() {
        return this.id;
    }
    
    public boolean isStatic() {
        return this.isStatic;
    }
    
    @Override
    public int hashCode() {
        return this.isStatic ? (-this.id) : this.id;
    }
    
    @Override
    public boolean equals(final Object o) {
        if (!(o instanceof Var)) {
            return false;
        }
        final Var v = (Var)o;
        return this.id == v.id && this.isStatic == v.isStatic;
    }
    
    @Override
    public String toString() {
        return this.getName();
    }
}
