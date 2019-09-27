package com.datasphere.source.sm;

import com.datasphere.common.constants.*;

public class DelimiterProcessor
{
    private Constant.recordstatus event;
    
    public DelimiterProcessor(final String[] _token, final Constant.recordstatus _event) {
        this.event = _event;
    }
    
    public Constant.recordstatus process() {
        return this.event;
    }
}
