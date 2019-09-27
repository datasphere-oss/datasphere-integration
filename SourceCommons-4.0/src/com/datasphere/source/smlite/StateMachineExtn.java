package com.datasphere.source.smlite;

import com.datasphere.source.lib.intf.*;
import com.datasphere.source.lib.reader.*;

public class StateMachineExtn extends StateMachine
{
    public StateMachineExtn(final Reader reader, final SMProperty prop) {
        super(reader, prop);
    }
    
    public StateMachineExtn(final SMProperty prop) {
        super(prop);
    }
    
    public StateMachineExtn(final SMProperty _prop, final SMCallback _callback) {
        super(_prop, _callback);
    }
    
    @Override
    public CharParser createParser(final SMProperty smProperty) {
        return new CharParserExtn(this, smProperty);
    }
    
    @Override
    protected boolean validateProperty() {
        return true;
    }
}
