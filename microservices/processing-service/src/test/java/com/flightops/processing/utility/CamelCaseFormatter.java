package com.flightops.processing.utility;

import org.jspecify.annotations.NullMarked;
import org.junit.jupiter.api.DisplayNameGenerator;

import java.lang.reflect.Method;
import java.util.List;

public class CamelCaseFormatter implements DisplayNameGenerator {

    @Override
    @NullMarked
    public String generateDisplayNameForClass(Class<?> testClass) {
        return replaceCapitals(testClass.getSimpleName());
    }

    @Override
    @NullMarked
    public String generateDisplayNameForNestedClass(List<Class<?>> enclosingInstanceTypes, Class<?> nestedClass) {
        return replaceCapitals(nestedClass.getSimpleName());
    }

    @Override
    @NullMarked
    public String generateDisplayNameForMethod(List<Class<?>> enclosingInstanceTypes, Class<?> testClass, Method testMethod) {
        return replaceCapitals(testMethod.getName());
    }

    private String replaceCapitals(String name) {
        // Insert space before capitals, but not at the start
        name = name.replaceAll("(?<!^)([A-Z])", " $1");
        // Insert space before numbers
        name = name.replaceAll("([0-9]+)", " $1");
        return name.toLowerCase().trim();
    }

}