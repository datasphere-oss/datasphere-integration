package com.datasphere.proc;

import java.util.*;
import com.datasphere.recovery.*;

public class PathManagerExtn extends PathManager
{
    private static final long serialVersionUID = -8657272415449722804L;
    
    public PathManagerExtn(final PathManager path) {
        super(path);
    }
    
    public boolean isFiltered(final Position that) {
        if (that != null && !this.isEmpty()) {
            for (final Path thatPath : that.values()) {
                final SourcePosition thatSP = thatPath.getLowSourcePosition();
                final SourcePosition thisSP = this.getLowPositionForPathSegment(thatPath);
                if (thisSP != null && thisSP.compareTo(thatSP) < 0) {
                    this.removePath(thatPath.getPathHash());
                    return false;
                }
            }
            return true;
        }
        return false;
    }
}
