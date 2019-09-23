package com.datasphere.runtime.components;

public interface IFlowComponent
{
    void setFlow(final Flow p0);
    
    Flow getFlow();
    
    Flow getTopLevelFlow();
    
    boolean recoveryIsEnabled();
}
