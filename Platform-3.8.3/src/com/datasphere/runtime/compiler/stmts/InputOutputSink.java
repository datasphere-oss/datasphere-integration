package com.datasphere.runtime.compiler.stmts;

import java.io.*;
import com.fasterxml.jackson.annotation.*;
import java.util.*;
import com.datasphere.runtime.compiler.*;

public interface InputOutputSink extends Serializable
{
    @JsonIgnore
    String getStreamName();
    
    @JsonIgnore
    MappedStream getGeneratedStream();
    
    @JsonIgnore
    List<String> getPartitionFields();
    
    @JsonIgnore
    boolean isFiltered();
    
    @JsonIgnore
    Select getFilter();
    
    @JsonIgnore
    List<TypeField> getTypeDefinition();
    
    @JsonIgnore
    String getFilterText();
}
