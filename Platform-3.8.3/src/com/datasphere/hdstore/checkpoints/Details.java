package com.datasphere.hdstore.checkpoints;

import com.datasphere.recovery.*;

final class Details
{
    public long hds;
    public long timestamp;
    public PathManager position;
    
    Details() {
        this.start();
        this.position = new PathManager();
    }
    
    Details(final Details copy) {
        this.hds = copy.hds;
        this.timestamp = copy.timestamp;
        this.position = new PathManager(copy.position);
    }
    
    public void start() {
        this.timestamp = System.currentTimeMillis();
        this.hds = 0L;
    }
}
