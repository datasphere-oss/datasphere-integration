package com.datasphere.metaRepository;

import java.util.Set;

import com.fasterxml.jackson.core.type.TypeReference;
import com.datasphere.uuid.UUID;

public class UUIDSetConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = -9060006939480717113L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<Set<UUID>>() {};
    }
}
