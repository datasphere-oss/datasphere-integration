package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;

public class StringSetConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = 6551168592266073269L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<Set<String>>() {};
    }
}
