package tofi.apinator.impl;

import jandcode.core.test.*;
import org.junit.jupiter.api.*;

import java.lang.reflect.*;
import java.util.*;

public class MethodGrabber_Test extends App_Test {

    public interface Int1 {
        void m1();

        void m2();
    }

    public static class Cls1 {
        public void m1() {
        }

        public void m2() {
        }

        protected void m2(int a) {
        }

        public static void m2static() {
        }

        private void m2priv() {
        }

        void m3() {
        }

    }

    void prn(Map<String, Method> lst) {
        for (var m : lst.entrySet()) {
            System.out.println(m.getKey() + ": " + m.getValue());
        }
    }

    ExcludeMethodsHolder excludeMethods;

    public void setUp() throws Exception {
        super.setUp();
        excludeMethods = new ExcludeMethodsHolder();
        excludeMethods.addClass(Object.class);
    }

    @Test
    public void int1() throws Exception {
        MethodGrabber g = new MethodGrabber(Int1.class, excludeMethods.getMethods());
        prn(g.getMethods());
    }

    @Test
    public void cls1() throws Exception {
        MethodGrabber g = new MethodGrabber(Cls1.class, excludeMethods.getMethods());
        prn(g.getMethods());
    }

}
