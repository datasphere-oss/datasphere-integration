package com.datasphere.persistence;

import java.util.*;

public class LRUList<T>
{
    Hashtable<T, ListElement<T>> listIndex;
    ListElement<T> head;
    ListElement<T> tail;
    
    public LRUList() {
        this.listIndex = new Hashtable<T, ListElement<T>>();
        this.head = null;
        this.tail = null;
    }
    
    public LRUList<T> add(final T item) {
        synchronized (this.listIndex) {
            ListElement<T> element = this.listIndex.get(item);
            if (element != null) {
                this.remove(element);
            }
            else {
                element = new ListElement<T>(item);
                this.listIndex.put(item, element);
            }
            this.insertBeginning(element);
        }
        return this;
    }
    
    public LRUList<T> remove(final T item) {
        synchronized (this.listIndex) {
            final ListElement<T> element = this.listIndex.get(item);
            if (element != null) {
                this.remove(element);
                this.listIndex.remove(item);
            }
        }
        return this;
    }
    
    public T removeLeastRecentlyUsed() {
        T item = null;
        synchronized (this.listIndex) {
            if (this.tail == null) {
                return null;
            }
            final ListElement<T> returnValue = this.tail;
            this.remove(this.tail);
            this.listIndex.remove(returnValue.item);
            item = returnValue.item;
        }
        return item;
    }
    
    private void insertBefore(final ListElement<T> element, final ListElement<T> newElement) {
        synchronized (this.listIndex) {
            newElement.prev = element.prev;
            newElement.next = element;
            if (element.prev == null) {
                this.head = newElement;
            }
            else {
                element.prev.next = newElement;
            }
            element.prev = newElement;
        }
    }
    
    protected void insertBeginning(final ListElement<T> newElement) {
        synchronized (this.listIndex) {
            if (this.head == null) {
                this.head = newElement;
                this.tail = newElement;
                newElement.prev = null;
                newElement.next = null;
            }
            else {
                this.insertBefore(this.head, newElement);
            }
        }
    }
    
    private void remove(final ListElement<T> element) {
        synchronized (this.listIndex) {
            if (element.prev == null) {
                this.head = element.next;
            }
            else {
                element.prev.next = element.next;
            }
            if (element.next == null) {
                this.tail = element.prev;
            }
            else {
                element.next.prev = element.prev;
            }
        }
    }
    
    public List<T> toList() {
        final ArrayList<T> retList = new ArrayList<T>();
        synchronized (this.listIndex) {
            for (ListElement<T> element = this.head; element != null; element = element.next) {
                retList.add(element.item);
            }
        }
        return retList;
    }
    
    public int size() {
        return this.listIndex.size();
    }
    
    private class ListElement<Y>
    {
        public ListElement<Y> prev;
        public ListElement<Y> next;
        public Y item;
        
        public ListElement(final Y item) {
            this.prev = null;
            this.next = null;
            this.item = item;
        }
    }
}
