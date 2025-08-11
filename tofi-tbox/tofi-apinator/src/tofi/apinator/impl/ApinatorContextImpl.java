package tofi.apinator.impl;

import jandcode.commons.conf.*;
import jandcode.core.*;
import tofi.apinator.*;

import java.lang.reflect.*;

public class ApinatorContextImpl implements ApinatorContext {

    private final BeanFactory beanFactory = new DefaultBeanFactory();
    private Object inst;
    private Object result;
    private Throwable exception;
    private final Method method;
    private final ApinatorInvoker invoker;
    private final long startTime;
    private final Conf conf;

    public ApinatorContextImpl(ApinatorInvoker invoker, Method method, BeanFactory parentBeanFactory, Conf conf) {
        this.conf = conf;
        this.invoker = invoker;
        this.method = method;
        this.startTime = System.currentTimeMillis();
        if (parentBeanFactory != null) {
            this.beanFactory.setParentBeanFactory(parentBeanFactory);
        }
    }

    public Conf getConf() {
        return conf;
    }

    public App getApp() {
        return this.invoker.getApp();
    }

    public BeanFactory getBeanFactory() {
        return beanFactory;
    }

    public Object getInst() {
        return inst;
    }

    public void setInst(Object inst) {
        this.inst = inst;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public Throwable getException() {
        return exception;
    }

    public void setException(Throwable exception) {
        this.exception = exception;
    }

    public Method getMethod() {
        return method;
    }

    public ApinatorInvoker getInvoker() {
        return invoker;
    }

    public long getStartTime() {
        return startTime;
    }

    public boolean hasError() {
        return this.exception != null;
    }

}
