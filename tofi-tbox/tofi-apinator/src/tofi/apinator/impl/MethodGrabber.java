package tofi.apinator.impl;

import jandcode.commons.error.*;

import java.lang.reflect.*;
import java.util.*;

/**
 * Сборщик методов для api
 */
public class MethodGrabber {

    private final Map<String, Method> methods = new LinkedHashMap<>();
    private final Class cls;
    private final Set<String> excludeMethods;

    public MethodGrabber(Class cls, Set<String> excludeMethods) {
        this.cls = cls;
        this.excludeMethods = excludeMethods;
        grab();
    }

    public Map<String, Method> getMethods() {
        return methods;
    }

    //////

    private void grab() {
        for (Method m : cls.getMethods()) {
            if (excludeMethods != null && excludeMethods.contains(m.getName())) {
                continue;
            }
            try {
                grabMethod(m);
            } catch (Exception e) {
                throw new XErrorMark(e, "class: " + cls.getName() + ", method: " + m);
            }
        }
    }

    private void grabMethod(Method m) {
        if (Modifier.isStatic(m.getModifiers())) {
            return;
        }
        if (m.getName().contains("$")) {
            return;
        }
        if (m.isDefault()) {
            throw new XError("В интерфейсе не разрешены default-методы");
        }
        if (methods.containsKey(m.getName())) {
            throw new XError("В интерфейсе не разрешены перегруженные методы");
        }
        methods.put(m.getName(), m);
    }

}
