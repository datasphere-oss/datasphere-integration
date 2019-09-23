package com.datasphere.hdstore;

import com.fasterxml.jackson.databind.node.*;

public class HD extends ObjectNode
{
    public HD() {
        super(Utility.nodeFactory);
    }
    
    public HD(final HD copy) {
        this();
        this.setAll((ObjectNode)copy);
    }
}
