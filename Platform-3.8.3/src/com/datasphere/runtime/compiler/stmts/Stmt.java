package com.datasphere.runtime.compiler.stmts;

import java.util.List;

import com.datasphere.metaRepository.MetaDataRepositoryException;
import com.datasphere.runtime.Property;
import com.datasphere.runtime.compiler.Compiler;
import com.datasphere.utility.Utility;

public abstract class Stmt
{
    public String sourceText;
    public List<Property> compileOptions;
    public List returnText;
    
    public abstract Object compile(final Compiler p0) throws MetaDataRepositoryException;
    
    public void setSourceText(final String txt, final List<Property> compileOptions) {
        this.sourceText = txt;
        this.compileOptions = compileOptions;
    }
    
    public boolean haveOption(final String opt) {
        if (this.compileOptions != null) {
            for (final Property s : this.compileOptions) {
                if (s.name.equalsIgnoreCase(opt)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    public Object getOption(final String opt) {
        if (this.compileOptions != null) {
            for (final Property s : this.compileOptions) {
                if (s.name.equalsIgnoreCase(opt)) {
                    return s.value;
                }
            }
        }
        return null;
    }
    
    @Override
    public String toString() {
        String str = this.sourceText;
        if (this.sourceText == null || this.sourceText.isEmpty()) {
            str += Utility.prettyPrintMap(this.compileOptions);
        }
        return str;
    }
    
    public String getRedactedSourceText() {
        return this.sourceText;
    }
}
