package tofi.apinator;

import jandcode.commons.conf.*;
import jandcode.commons.named.*;
import jandcode.core.*;

public interface ApinatorApi extends Comp, IConfLink {

    ApinatorInvoker getInvoker();

    NamedList<ApinatorApiItem> getItems();

    /**
     * Получить экземпляр для интерфейса
     *
     * @param clsIntfName имя интерфейса
     */
    Object get(String clsIntfName);

    /**
     * Получить экземпляр для интерфейса
     *
     * @param clsIntf имя интерфейса
     */
    <A> A get(Class<A> clsIntf);

}
