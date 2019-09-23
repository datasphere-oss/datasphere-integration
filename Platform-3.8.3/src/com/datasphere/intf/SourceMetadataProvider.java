package com.datasphere.intf;

import java.util.*;
import com.datasphere.runtime.compiler.*;

public interface SourceMetadataProvider
{
    String getMetadataKey();
    
    Map<String, TypeDefOrName> getMetadata() throws Exception;
}
