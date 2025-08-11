package tofi.api.mdl;

import jandcode.core.store.Store;

import java.util.Map;


public interface ApiMetaCube {

    /**
     *
     * @param dimPeriod id DimPeriod
     * @param curDate current date
     * @return Store DimPeriod
     */
    Store loadDimPeriod(long dimPeriod, String curDate);

    void saveCubeInfo(Map<String, Object> params);


}
