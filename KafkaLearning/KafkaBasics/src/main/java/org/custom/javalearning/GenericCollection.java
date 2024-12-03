package org.custom.javalearning;

import java.util.Iterator;

public class GenericCollection<T> implements Iterable<T> {
    private final T[] items;

    public GenericCollection(T[] items) {

        this.items = items;
    }

    @Override
    public Iterator<T> iterator() {
        return new GenericIterator<>(items);
    }
}
