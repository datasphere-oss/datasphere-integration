package com.datasphere.metaRepository;

import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.datasphere.uuid.UUID;

public class PropertyListConverter extends PropertyDefConverter
{
    public static final long serialVersionUID = 2740988897547957681L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<UUID>>() {};
    }
}
