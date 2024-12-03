package org.custom.javalearning;

public class GenericPrinter<T,V> {
    T genericValue;
    V genericType;
    public GenericPrinter(T genericValue, V genericType) {
        this.genericValue = genericValue;
        this.genericType = genericType;
    }

    public void run() {
        System.out.println("Shouting the variable: " + genericValue);
        System.out.println("Shouting the type: " + genericType);
    }

    public T genericMethod(T t, V v) {
        System.out.println("Hello: " + t + v);
        return t;
    }

}
