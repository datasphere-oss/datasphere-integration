package com.datasphere.runtime.compiler.stmts;

import java.util.*;
import com.datasphere.runtime.compiler.*;

public class InputClause implements InputOutputSink
{
    String stream;
    MappedStream mp;
    List<String> part;
    Select select;
    
    public InputClause(final String stream, final MappedStream mp, final List<String> part, final Select select) {
        this.stream = stream;
        this.mp = mp;
        this.part = part;
        this.select = select;
    }
    
    @Override
    public String getStreamName() {
        return this.stream;
    }
    
    @Override
    public MappedStream getGeneratedStream() {
        return this.mp;
    }
    
    @Override
    public List<String> getPartitionFields() {
        return this.part;
    }
    
    @Override
    public boolean isFiltered() {
        return this.select != null;
    }
    
    @Override
    public Select getFilter() {
        return this.select;
    }
    
    @Override
    public List<TypeField> getTypeDefinition() {
        return null;
    }
    
    @Override
    public String getFilterText() {
        return null;
    }
}
