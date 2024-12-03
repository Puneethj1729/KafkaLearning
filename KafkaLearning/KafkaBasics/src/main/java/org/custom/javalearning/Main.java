package org.custom.javalearning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) {
        GenericPrinter<Integer, String> integerPrinter = new GenericPrinter<> (34, "Integer");
        GenericPrinter<Double, String> doublePrinter = new GenericPrinter<>(56.44, "Double");
        integerPrinter.run();
        doublePrinter.run();
        System.out.println(integerPrinter.genericMethod(10,"Double"));

        // Generic List and Iterator.

        String[] names = {"Alice", "Bob", "Charlie"};
        GenericCollection<String> nameCollection = new GenericCollection<>(names);
        for (String name : nameCollection) {
            System.out.println(name);
        }

        Integer[] numbers = {1, 2, 3, 4, 5};
        GenericCollection<Integer> numberCollection = new GenericCollection<>(numbers);
        for (Integer number : numberCollection) {
            System.out.println(number);
        }
    }
}
