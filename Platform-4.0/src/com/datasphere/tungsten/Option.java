package com.datasphere.tungsten;

public class Option
{
    String option;
    Object state;
    
    public Option(final String optionIn, final Object stateIn) {
        this.option = optionIn;
        this.state = stateIn;
    }
    
    public String getOption() {
        return this.option;
    }
    
    public void setOption(final String option) {
        this.option = option;
    }
    
    public Object getState() {
        return this.state;
    }
    
    public void setState(final Object state) {
        this.state = state;
    }
}
