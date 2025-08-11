package tofi.apinator;

import jandcode.core.test.*;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.*;
import org.junit.jupiter.api.*;

import java.lang.reflect.*;
import java.util.*;

public class Proxy1_Test extends App_Test {

    public interface Int1 {
        void m1();

        void m2(String s1);
    }

    public static class Cls1 {

        public void m1() {
            m3priv();
        }

        public void m2(String s1) {

        }

        private void m3priv() {
            System.out.println("m3priv called");
        }

    }


    @Test
    public void interface1() throws Exception {
        //String[] a = new String[]{};
        ProxyFactory f = new ProxyFactory();
        f.setInterfaces(new Class[]{Int1.class});
        Class clsProxy = f.createClass();
        System.out.println(clsProxy);

        //
        Object res = clsProxy.getDeclaredConstructor().newInstance();
        ((Proxy) res).setHandler(new MethodHandler() {
            public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
                System.out.println("thisMethod: " + thisMethod);
                System.out.println("proceed: " + proceed);
                //System.out.println("self: " + self);
                System.out.println("args: " + Arrays.stream(args).toList());
                return null;
            }
        });

        ((Int1) res).m1();
        ((Int1) res).m2("eee");

    }

    @Test
    public void class1() throws Exception {
        //String[] a = new String[]{};
        ProxyFactory f = new ProxyFactory();
        f.setSuperclass(Cls1.class);
        Class clsProxy = f.createClass();
        System.out.println(clsProxy);

        //
        Object res = clsProxy.getDeclaredConstructor().newInstance();
        ((Proxy) res).setHandler(new MethodHandler() {
            public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
                System.out.println("thisMethod: " + thisMethod);

                for (var m1 : Cls1.class.getMethods()) {
                    if (m1.equals(thisMethod)) {
                        System.out.println("EQUAL!!!!!: " + m1);
                    }
                }

                System.out.println("proceed: " + proceed);
                //System.out.println("self: " + self);
                System.out.println("args: " + Arrays.stream(args).toList());
                if (proceed != null) {
                    return proceed.invoke(self, args);
                }
                return null;
            }
        });

//        utils.delim();
//        for (var m: clsProxy.getMethods()){
//            System.out.println(m);
//        }
//        utils.delim();


        ((Cls1) res).m1();
        ((Cls1) res).m2("eee");
        res.toString();

    }


}
