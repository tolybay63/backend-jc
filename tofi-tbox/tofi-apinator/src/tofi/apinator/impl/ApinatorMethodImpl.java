package tofi.apinator.impl;

import jandcode.commons.named.*;
import tofi.apinator.*;

import java.lang.reflect.*;

public class ApinatorMethodImpl implements ApinatorMethod {

    private final Method method;
    private final NamedList<ApinatorMethodParam> params = new DefaultNamedList<>();

    public ApinatorMethodImpl(Method method) {
        this.method = method;
        grabParams();
    }

    protected void grabParams() {
        Parameter[] prms = this.method.getParameters();
        for (int i = 0; i < prms.length; i++) {
            Parameter p = prms[i];
            ApinatorMethodParam mp = new ApinatorMethodParamImpl(p.getName(), p, i);
            this.params.add(mp);
        }
    }

    public Method getMethod() {
        return method;
    }

    public String getName() {
        return method.getName();
    }

    public NamedList<ApinatorMethodParam> getParams() {
        return params;
    }

}
