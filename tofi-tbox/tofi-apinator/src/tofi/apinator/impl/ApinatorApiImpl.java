package tofi.apinator.impl;

import jandcode.commons.conf.*;
import jandcode.commons.error.*;
import jandcode.commons.named.*;
import jandcode.core.*;
import tofi.apinator.*;

import java.lang.reflect.*;
import java.util.*;

public class ApinatorApiImpl extends BaseComp implements ApinatorApi, IApinatorInvoker {

    private final NamedList<ApinatorApiItem> items = new DefaultNamedList<>();
    private final ProxyManager proxyManager = new ProxyManager();
    private ApinatorInvoker invoker;
    private Conf conf;

    protected void onConfigure(BeanConfig cfg) throws Exception {
        super.onConfigure(cfg);
        //
        this.conf = cfg.getConf();

        items.setNotFoundMessage("Не найден класс [{0}] в api [{1}]", this.getName());

        Set<String> excludeMethods = getApp().bean(ApinatorExcludeMethodsService.class).getMethods();
        for (Conf itemConf : cfg.getConf().getConfs("item")) {
            ApinatorApiItem it = new ApinatorApiItemImpl(this, itemConf, excludeMethods);
            items.add(it);
        }

    }

    public Conf getConf() {
        return conf;
    }

    public ApinatorInvoker getInvoker() {
        return invoker;
    }

    public void setInvoker(ApinatorInvoker invoker) {
        this.invoker = invoker;
    }

    public NamedList<ApinatorApiItem> getItems() {
        return items;
    }

    public Object get(String clsIntfName) {
        ApinatorApiItem ai = this.items.get(clsIntfName);
        try {
            return this.proxyManager.createInst(ai, this);
        } catch (Exception e) {
            throw new XErrorMark(e, "create: " + clsIntfName);
        }
    }

    public <A> A get(Class<A> clsIntf) {
        return (A) get(clsIntf.getName());
    }

    public Object invokeApi(Class cls, Method method, Conf contextConf, Object... args) throws Exception {
        return getInvoker().invokeApi(cls, method, contextConf, args);
    }

}
