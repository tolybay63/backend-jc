package tofi.userdata.metadao

import groovy.transform.CompileStatic
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService


@CompileStatic
class MetaDao extends BaseMdbUtils {

    ApinatorApi apiMeta(){
        return app.bean(ApinatorService).getApi("meta")
    }

    @DaoMethod
    Store loadDictAsStore(String dictName) throws Exception {
        return apiMeta().get(ApiMeta).loadDictAsStore(dictName)
    }

    @DaoMethod
    Map<Long, String> loadDict(String dictName) throws Exception {
        return apiMeta().get(ApiMeta).loadDict(dictName)
    }

    @DaoMethod
    public Store loadFactorSexValues() throws Exception {
        return apiMeta().get(ApiMeta).loadFactorVals("Factor_Sex")
    }

    @DaoMethod
    public Map<String, Store> infoFlatTable(String ft_cod) {
        return apiMeta().get(ApiMeta).infoFlatTable(ft_cod)
    }

    @DaoMethod
    public Store loadClsTree(Map<String, Object> params) throws Exception {
        return apiMeta().get(ApiMeta).loadClsTree(params)
    }

}

