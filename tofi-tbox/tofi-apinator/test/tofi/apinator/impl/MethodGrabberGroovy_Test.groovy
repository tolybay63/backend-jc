package tofi.apinator.impl

import jandcode.core.test.*
import org.junit.jupiter.api.*

import java.lang.reflect.*

class MethodGrabberGroovy_Test extends App_Test {

    interface Int1 {
        void m1();

        void m2();
    }

    static class Cls1 {
        void m1() {
        }

        void m2() {
        }

        protected void m2(int a) {
        }

        static void m2static() {
        }

        private void m2priv() {
        }

        void m3() {
        }

    }

    void prn(Map<String, Method> lst) {
        for (var m : lst.entrySet()) {
            System.out.println(m.getKey() + ": " + m.getValue())
        }
    }

    ExcludeMethodsHolder excludeMethods

    void setUp() throws Exception {
        super.setUp()
        excludeMethods = new ExcludeMethodsHolder()
        excludeMethods.addClass(Object.class)
        excludeMethods.addClass(GroovyObject.class)
        excludeMethods.addMethod("propertyMissing")
        excludeMethods.addMethod("methodMissing")
    }

    @Test
    void int1() throws Exception {
        MethodGrabber g = new MethodGrabber(Int1.class, excludeMethods.getMethods())
        prn(g.getMethods())
    }

    @Test
    void cls1() throws Exception {
        MethodGrabber g = new MethodGrabber(Cls1.class, excludeMethods.getMethods())
        prn(g.getMethods())
    }

    @Test
    void cls1anal() throws Exception {
        for (m in Cls1.getMethods()) {
            println m
        }
    }


}
