package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;

public class LinkedMapConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = 1061473287059512670L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<LinkedHashMap<String, String>>() {};
    }
}
