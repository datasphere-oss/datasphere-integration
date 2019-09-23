package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;

public class StringListConverter extends PropertyDefConverter
{
    public static final long serialVersionUID = 2740988897547657681L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<String>>() {};
    }
}
