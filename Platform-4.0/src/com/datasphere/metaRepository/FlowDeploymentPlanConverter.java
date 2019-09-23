package com.datasphere.metaRepository;

import com.fasterxml.jackson.core.type.*;
import java.util.*;
import com.datasphere.runtime.meta.*;

public class FlowDeploymentPlanConverter extends PropertyDefConverter
{
    public static final long serialVersionUID = 2740988897547657681L;
    
    @Override
    TypeReference<?> getTypeReference() {
        return new TypeReference<List<MetaInfo.Flow.Detail>>() {};
    }
}
