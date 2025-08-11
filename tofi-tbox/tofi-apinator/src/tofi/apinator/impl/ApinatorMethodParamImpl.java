package tofi.apinator.impl;

import tofi.apinator.*;

import java.lang.reflect.*;

public class ApinatorMethodParamImpl implements ApinatorMethodParam {

    private final String name;
    private final Parameter parameter;
    private final Class<?> type;
    private final int index;

    public ApinatorMethodParamImpl(String name, Parameter parameter, int index) {
        this.name = name;
        this.parameter = parameter;
        this.type = parameter.getType();
        this.index = index;
    }

    public String getName() {
        return name;
    }

    public Parameter getParameter() {
        return parameter;
    }

    public Class<?> getType() {
        return type;
    }

    public int getIndex() {
        return index;
    }

}
