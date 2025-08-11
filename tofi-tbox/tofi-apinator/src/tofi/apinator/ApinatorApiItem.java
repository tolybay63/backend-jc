package tofi.apinator;

import jandcode.commons.conf.*;
import jandcode.commons.named.*;
import jandcode.core.*;

import java.util.*;

public interface ApinatorApiItem extends Comp, IConfLink {

    /**
     * Какому api принадлежит
     */
    ApinatorApi getApi();

    Class getClsIntf();

    Class getClsImpl();

    NamedList<ApinatorMethod> getMethods();

    Set<String> getMethodNames();

}
