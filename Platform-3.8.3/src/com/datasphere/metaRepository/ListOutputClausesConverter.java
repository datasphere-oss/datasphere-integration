package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;
import com.datasphere.runtime.compiler.stmts.*;

public class ListOutputClausesConverter extends PropertyDefConverter
{
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<OutputClause>>() {};
    }
}
