package com.datasphere.runtime.compiler.stmts;

import java.util.*;
import com.datasphere.runtime.compiler.*;
import com.fasterxml.jackson.annotation.*;
import com.datasphere.utility.*;

public class OutputClause implements InputOutputSink
{
    public String stream;
    public MappedStream mp;
    public List<String> part;
    public List<TypeField> fields;
    @JsonIgnore
    private transient Select select;
    public String selectTQL;
    
    public OutputClause() {
    }
    
    public OutputClause(final String stream, final MappedStream mp, final List<String> part, final List<TypeField> fields, final Select select, final String selectTQL) {
        this.stream = stream;
        this.mp = mp;
        this.part = part;
        this.fields = fields;
        this.select = select;
        this.selectTQL = selectTQL;
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
        return this.fields;
    }
    
    @Override
    public String getFilterText() {
        return this.selectTQL;
    }
    
    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        if (this.stream != null) {
            sb.append(this.stream + " ");
        }
        if (this.mp != null) {
            sb.append(this.mp + " ");
        }
        if (this.selectTQL != null) {
            sb.append(Utility.cleanUpSelectStatement(this.selectTQL));
        }
        final String str = sb.toString().replaceAll("[\u0000-\u001f]", "");
        return str;
    }
}
