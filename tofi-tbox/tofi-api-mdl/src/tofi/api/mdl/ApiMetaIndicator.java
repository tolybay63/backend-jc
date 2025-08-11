package tofi.api.mdl;

import jandcode.core.store.Store;

import java.util.Map;

public interface ApiMetaIndicator {

    /**
     *
     * @param cls класс
     * @return Store
     */
    Store loadMultiProp(long cls);

    /**
     *
     * @param multiprop Multi Prop
     * @return Store
     */
    Store loadDimMultiPropItem(long multiprop);

    /**
     *
     * @param multiprop id MultiProp
     * @return Store
     */
    Store loadMultiPropDim(long multiprop);

}
