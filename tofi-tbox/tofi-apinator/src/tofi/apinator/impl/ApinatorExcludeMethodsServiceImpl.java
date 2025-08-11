package tofi.apinator.impl;

import jandcode.commons.*;
import jandcode.commons.conf.*;
import jandcode.core.*;
import tofi.apinator.*;

import java.util.*;

public class ApinatorExcludeMethodsServiceImpl extends BaseComp implements ApinatorExcludeMethodsService {

    private final ExcludeMethodsHolder excludeMethodsHolder = new ExcludeMethodsHolder();

    protected void onConfigure(BeanConfig cfg) throws Exception {
        super.onConfigure(cfg);
        //
        Conf rootConf = getApp().getConf().getConf("apinator");

        //
        for (Conf x : rootConf.getConfs("excludeMethods")) {
            String nm = x.getName();
            if (nm.contains(".")) {
                try {
                    this.excludeMethodsHolder.addClass(UtClass.getClass(nm));
                } catch (Exception e) {
                    // ignore
                    // возможно такого класса и нет, если не подключена какая нибудь jar
                }
            } else {
                this.excludeMethodsHolder.addMethod(nm);
            }
        }

    }

    public Set<String> getMethods() {
        return excludeMethodsHolder.getMethods();
    }
}
