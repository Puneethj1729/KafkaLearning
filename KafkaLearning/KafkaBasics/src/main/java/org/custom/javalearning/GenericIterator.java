package org.custom.javalearning;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class GenericIterator<T> implements Iterator<T> {
    private final T[] items;
    private int index = 0;

    public GenericIterator(T[] items) {
        this.items = items;
    }

    @Override
    public boolean hasNext() {
        return index < items.length;
    }

    @Override
    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more elements");
        }
        return items[index++];
    }
}
