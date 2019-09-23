package com.datasphere.runtime.exceptions;

import com.datasphere.exception.*;

public class UnterminatedTokenException extends CompilationException
{
    private static final long serialVersionUID = 7397745568373236642L;
    public final String term;
    
    public UnterminatedTokenException(final String message, final String term) {
        super(message);
        this.term = term;
    }
}
