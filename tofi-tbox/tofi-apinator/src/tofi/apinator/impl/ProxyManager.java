package tofi.apinator.impl;

import jandcode.commons.*;
import jandcode.commons.conf.*;
import jandcode.commons.error.*;
import javassist.util.proxy.Proxy;
import javassist.util.proxy.*;
import tofi.apinator.*;

import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * Cоздание proxy-экземпляров
 */
public class ProxyManager {

    // не api-методы, которые можно вызывать для экземпляра dao
    // вынужденная мера, groovy например дергает getMetaClass,
    // когда вызов groovy-api идет из groovy-метода
    private static final Set<String> enableNotApiMethods = new HashSet<>();

    static {
        enableNotApiMethods.add("getMetaClass");
    }

    protected Map<Class, Class> proxyClasses = new ConcurrentHashMap<>();

    class ApiMethodHandler implements MethodHandler {

        private final ApinatorApiItem ai;
        private final IApinatorInvoker invoker;
        private final Conf contextConf;

        public ApiMethodHandler(ApinatorApiItem ai, IApinatorInvoker invoker) {
            this.ai = ai;
            this.invoker = invoker;
            // собираем конфигурацию для контекста как атрибуты api и конфигурацию item
            this.contextConf = Conf.create(ai.getClsIntf().getName());
            this.contextConf.join(UtConf.getAttrs(ai.getApi().getConf()));
            this.contextConf.join(ai.getConf());
        }

        public Object invoke(Object self, Method thisMethod, Method proceed, Object[] args) throws Throwable {
            if (this.ai.getMethodNames().contains(thisMethod.getName())) {
                return invoker.invokeApi(ai.getClsImpl(), thisMethod, contextConf, args);
            } else {
                if (enableNotApiMethods.contains(thisMethod.getName())) {
                    return proceed.invoke(self, args);
                }
                throw new XError("Метод [{0}] не является методом api", thisMethod);
            }
        }

    }

    /**
     * Создать экземпляр proxy-класса для доступа к api
     *
     * @param ai      для какого item
     * @param invoker кто будет фактически выполнять методы
     * @return экземпляр proxy-класса для clsIntf
     */
    public Object createInst(ApinatorApiItem ai, IApinatorInvoker invoker) throws Exception {
        Class proxyClass = getProxyClass(ai.getClsIntf());
        Object res = proxyClass.getDeclaredConstructor().newInstance();
        ((Proxy) res).setHandler(new ApiMethodHandler(ai, invoker));
        return res;
    }

    //////

    private Class getProxyClass(Class clsIntf) {
        Class res = proxyClasses.get(clsIntf);
        if (res == null) {
            synchronized (this) {
                res = proxyClasses.get(clsIntf);
                if (res == null) {
                    res = createProxyClass(clsIntf);
                    proxyClasses.put(clsIntf, res);
                }
            }
        }
        return res;
    }

    private Class createProxyClass(Class clsIntf) {
        ProxyFactory f = new ProxyFactory();
        if (clsIntf.isInterface()) {
            f.setInterfaces(new Class[]{clsIntf});
        } else {
            f.setSuperclass(clsIntf);
        }
        return f.createClass();
    }

}
