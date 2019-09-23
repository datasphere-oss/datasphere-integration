package com.datasphere.metaRepository;

import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.datasphere.uuid.UUID;

public class UUIDListConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = -5546356012011481582L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<UUID>>() {};
    }
}
