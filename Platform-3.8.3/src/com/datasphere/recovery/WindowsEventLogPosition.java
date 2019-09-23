package com.datasphere.recovery;

public class WindowsEventLogPosition extends SourcePosition
{
    private static final long serialVersionUID = -5432586375077517449L;
    private int eventRecordNumberOffset;
    
    public WindowsEventLogPosition(final int offset) {
        this.eventRecordNumberOffset = offset;
    }
    
    public int getOffset() {
        return this.eventRecordNumberOffset;
    }
    
    public int compareTo(final SourcePosition otherPosition) {
        if (otherPosition != null && otherPosition instanceof WindowsEventLogPosition) {
            final WindowsEventLogPosition otherWindowsPosition = (WindowsEventLogPosition)otherPosition;
            final int otherOffset = otherWindowsPosition.getOffset();
            final int cmp = (this.eventRecordNumberOffset > otherOffset) ? 1 : ((this.eventRecordNumberOffset < otherOffset) ? -1 : 0);
            return cmp;
        }
        return 1;
    }
}
