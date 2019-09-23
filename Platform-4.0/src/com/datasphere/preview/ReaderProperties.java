package com.datasphere.preview;

import java.util.*;

public interface ReaderProperties
{
    Map<String, Object> getDefaultProperties();
    
    Map<String, String> getDefaultPropertiesAsString();
}
