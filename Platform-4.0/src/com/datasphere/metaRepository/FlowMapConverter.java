package com.datasphere.metaRepository;

import java.util.LinkedHashSet;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.datasphere.runtime.components.EntityType;
import com.datasphere.uuid.UUID;

public class FlowMapConverter extends PropertyDefConverter
{
    public static final long serialVersionUID = 2740988897547657681L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<Map<EntityType, LinkedHashSet<UUID>>>() {};
    }
}
