package tofi.api.mdl.utils.dimPeriod.helpers;

import jandcode.core.store.Store;

import java.util.Map;

public interface IStoreLoader {

    /**
     * Загружает сторе по параметру
     *
     * @param params Map
     * @return Store
     */
    Store loadInitialStore(Map<String, Object> params) throws Exception;

}
