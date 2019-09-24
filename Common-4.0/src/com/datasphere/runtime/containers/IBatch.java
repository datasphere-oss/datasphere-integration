package com.datasphere.runtime.containers;

import java.io.*;
import java.util.*;

public interface IBatch<T> extends Iterable<T>, Serializable
{
    Iterator<T> iterator();
    
    int size();
    
    boolean isEmpty();
    
    T first();
    
    T last();
}
