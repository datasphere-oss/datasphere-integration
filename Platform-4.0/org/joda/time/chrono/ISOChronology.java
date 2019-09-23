package org.joda.time.chrono;

import org.joda.time.field.*;
import java.util.*;
import java.io.*;
import org.joda.time.*;

public final class ISOChronology extends AssembledChronology
{
    private static final long serialVersionUID = -6212696554273812441L;
    private static final ISOChronology INSTANCE_UTC;
    private static final int FAST_CACHE_SIZE = 64;
    private static final ISOChronology[] cFastCache;
    private static final Map<DateTimeZone, ISOChronology> cCache;
    
    public static ISOChronology getInstanceUTC() {
        return ISOChronology.INSTANCE_UTC;
    }
    
    public static ISOChronology getInstance() {
        return getInstance(DateTimeZone.getDefault());
    }
    
    public static ISOChronology getInstance(DateTimeZone zone) {
        if (zone == null) {
            zone = DateTimeZone.getDefault();
        }
        final int index = System.identityHashCode(zone) & 0x3F;
        ISOChronology chrono = ISOChronology.cFastCache[index];
        if (chrono != null && chrono.getZone() == zone) {
            return chrono;
        }
        synchronized (ISOChronology.cCache) {
            chrono = ISOChronology.cCache.get(zone);
            if (chrono == null) {
                chrono = new ISOChronology((Chronology)ZonedChronology.getInstance((Chronology)ISOChronology.INSTANCE_UTC, zone));
                ISOChronology.cCache.put(zone, chrono);
            }
        }
        return ISOChronology.cFastCache[index] = chrono;
    }
    
    public ISOChronology() {
        super((Chronology)null, (Object)null);
    }
    
    private ISOChronology(final Chronology base) {
        super(base, (Object)null);
    }
    
    public Chronology withUTC() {
        return (Chronology)ISOChronology.INSTANCE_UTC;
    }
    
    public Chronology withZone(DateTimeZone zone) {
        if (zone == null) {
            zone = DateTimeZone.getDefault();
        }
        if (zone == this.getZone()) {
            return (Chronology)this;
        }
        return (Chronology)getInstance(zone);
    }
    
    public String toString() {
        String str = "ISOChronology";
        final DateTimeZone zone = this.getZone();
        if (zone != null) {
            str = str + '[' + zone.getID() + ']';
        }
        return str;
    }
    
    protected void assemble(final AssembledChronology.Fields fields) {
        if (this.getBase() == null) {
            return;
        }
        if (this.getBase().getZone() == DateTimeZone.UTC) {
            fields.centuryOfEra = (DateTimeField)new DividedDateTimeField(ISOYearOfEraDateTimeField.INSTANCE, DateTimeFieldType.centuryOfEra(), 100);
            fields.yearOfCentury = (DateTimeField)new RemainderDateTimeField((DividedDateTimeField)fields.centuryOfEra, DateTimeFieldType.yearOfCentury());
            fields.weekyearOfCentury = (DateTimeField)new RemainderDateTimeField((DividedDateTimeField)fields.centuryOfEra, DateTimeFieldType.weekyearOfCentury());
            fields.centuries = fields.centuryOfEra.getDurationField();
        }
    }
    
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj instanceof ISOChronology) {
            final ISOChronology chrono = (ISOChronology)obj;
            return this.getZone().equals((Object)chrono.getZone());
        }
        return false;
    }
    
    public int hashCode() {
        return "ISO".hashCode() * 11 + this.getZone().hashCode();
    }
    
    private Object writeReplace() {
        return new Stub(this.getZone());
    }
    
    static {
        cCache = new HashMap<DateTimeZone, ISOChronology>();
        cFastCache = new ISOChronology[64];
        INSTANCE_UTC = new ISOChronology((Chronology)GregorianChronology.getInstanceUTC());
        ISOChronology.cCache.put(DateTimeZone.UTC, ISOChronology.INSTANCE_UTC);
    }
    
    private static final class Stub implements Serializable
    {
        private static final long serialVersionUID = -6212696554273812441L;
        private transient DateTimeZone iZone;
        
        Stub(final DateTimeZone zone) {
            this.iZone = zone;
        }
        
        private Object readResolve() {
            return ISOChronology.getInstance(this.iZone);
        }
        
        private void writeObject(final ObjectOutputStream out) throws IOException {
            out.writeObject(this.iZone);
        }
        
        private void readObject(final ObjectInputStream in) throws IOException, ClassNotFoundException {
            this.iZone = (DateTimeZone)in.readObject();
        }
    }
}
