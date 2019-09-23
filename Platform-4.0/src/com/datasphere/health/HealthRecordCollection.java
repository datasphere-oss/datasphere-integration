package com.datasphere.health;

import com.datasphere.hdstore.*;
import java.util.*;

public class HealthRecordCollection
{
    public final Set<HD> healthRecords;
    public String next;
    public String prev;
    
    public HealthRecordCollection() {
        this.healthRecords = new HashSet<HD>();
    }
}
