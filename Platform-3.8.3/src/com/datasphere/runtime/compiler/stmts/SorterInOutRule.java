package com.datasphere.runtime.compiler.stmts;

public class SorterInOutRule
{
    public final String inStream;
    public final String inStreamField;
    public final String outStream;
    
    public SorterInOutRule(final String inStream, final String inStreamField, final String outStream) {
        this.inStream = inStream;
        this.inStreamField = inStreamField;
        this.outStream = outStream;
    }
    
    @Override
    public String toString() {
        return this.inStream + " ON " + this.inStreamField + " OUPUT TO " + this.outStream;
    }
}
