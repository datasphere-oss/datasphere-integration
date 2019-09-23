package com.datasphere.persistence;

import java.util.*;
import org.eclipse.persistence.jpa.*;
import javax.persistence.*;
import org.eclipse.persistence.queries.*;

public class JPAIterable<T> implements Iterable<T>
{
    EntityManager em;
    Query query;
    Iterable<T> it;
    boolean isNative;
    boolean noEvents;
    
    public JPAIterable(final EntityManager em, final Query query, final boolean isNative, final boolean noEvents) {
        this.em = em;
        this.query = query;
        this.isNative = isNative;
        this.noEvents = noEvents;
        this.it = this.getResults(query);
    }
    
    @Override
    public Iterator<T> iterator() {
        return this.it.iterator();
    }
    
    public void close() {
        closeResults(this.it);
        this.em.clear();
        this.em.close();
    }
    
    private <T> Iterable<T> getResults(final Query query) {
        if (query instanceof JpaQuery) {
            final JpaQuery<T> jQuery = (JpaQuery<T>)query;
            jQuery.setHint("eclipselink.cursor.scrollable.result-set-type", (Object)"ScrollInsensitive").setHint("eclipselink.cursor.scrollable", (Object)true).setHint("eclipselink.cursor.page-size", (Object)1000);
            jQuery.setHint("eclipselink.cache-usage", (Object)"NoCache").setHint("eclipselink.read-only", (Object)"True").setHint("javax.persistence.cache.storeMode", (Object)CacheStoreMode.BYPASS);
            if (this.noEvents) {
                jQuery.setHint("eclipselink.fetch-group.name", (Object)"noEvents");
            }
            final Cursor cursor = jQuery.getResultCursor();
            return new Iterable<T>() {
                @Override
                public Iterator<T> iterator() {
                    return (Iterator<T>)cursor;
                }
            };
        }
        return (Iterable<T>)query.getResultList();
    }
    
    static void closeResults(final Iterable<?> list) {
        if (list.iterator() instanceof Cursor) {
            ((Cursor)list.iterator()).close();
        }
    }
}
