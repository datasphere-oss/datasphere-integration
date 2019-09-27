package com.datasphere.source.lib.intf;

import com.datasphere.source.smlite.*;

public interface SMCallback
{
    boolean onEvent(final SMEvent p0);
    
    void onEvent(final ColumnEvent p0);
    
    void onEvent(final RowEvent p0);
    
    void onEvent(final QuoteEvent p0);
    
    void onEvent(final QuoteBeginEvent p0);
    
    void onEvent(final QuoteEndEvent p0);
    
    void onEvent(final ResetEvent p0);
    
    void onEvent(final RowBeginEvent p0);
    
    void onEvent(final RowEndEvent p0);
    
    void onEvent(final TimeStampEvent p0);
    
    void onEvent(final EndOfBlockEvent p0);
    
    void onEvent(final EscapeEvent p0);
    
    void onEvent(final CommentEvent p0);
    
    void onEvent(final NVPEvent p0);
}
