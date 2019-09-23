package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;
import com.datasphere.runtime.meta.*;

public class RolesListConverter extends PropertyDefConverter
{
    private static final long serialVersionUID = 1061473287059512670L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<MetaInfo.Role>>() {};
    }
}
