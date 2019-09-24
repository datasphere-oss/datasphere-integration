package com.datasphere.recovery;

public class NumberSourcePosition extends SourcePosition
{
    private static final long serialVersionUID = -6945135854956636769L;
    public final long value;
    
    public NumberSourcePosition(final long value) {
        this.value = value;
    }
    
    @Override
    public int compareTo(final SourcePosition arg0) {
        if (!(arg0 instanceof NumberSourcePosition)) {
            throw new IllegalArgumentException("Require LongSourcePosition but got " + arg0);
        }
        final NumberSourcePosition lsp = (NumberSourcePosition)arg0;
        final long diff = this.value - lsp.value;
        if (diff < 0L) {
            return -1;
        }
        if (diff > 0L) {
            return 1;
        }
        return 0;
    }
    
    @Override
    public String toString() {
        return "LongSourcePosition value=" + this.value;
    }
    
    @Override
    public boolean equals(final Object object) {
        if (!(object instanceof NumberSourcePosition)) {
            return false;
        }
        final NumberSourcePosition that = (NumberSourcePosition)object;
        return this.value == that.value;
    }
    
    @Override
    public int hashCode() {
        return (int)(this.value ^ this.value >>> 32);
    }
}
