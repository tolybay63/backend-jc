package tofi.apinator.impl;

import java.lang.reflect.*;
import java.util.*;

/**
 * Набор методов, которые не должны попадать в api
 */
public class ExcludeMethodsHolder {

    private final Set<String> methods = new HashSet<>();

    public Set<String> getMethods() {
        return methods;
    }

    /**
     * Добавить конкретный метод
     */
    public void addMethod(Method method) {
        methods.add(method.getName());
    }

    /**
     * Добавить конкретный метод
     */
    public void addMethod(String methodName) {
        methods.add(methodName);
    }

    /**
     * Добавить все методы из класса
     */
    public void addClass(Class cls) {
        for (var m : cls.getMethods()) {
            methods.add(m.getName());
        }
    }

}
