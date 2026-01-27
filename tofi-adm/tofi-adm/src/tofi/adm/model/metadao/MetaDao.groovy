package tofi.adm.model.metadao
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class MetaDao extends BaseMdbUtils {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    @DaoMethod
    Store loadDictAsStore(String dictName) {
        return apiMeta().get(ApiMeta).loadDictAsStore(dictName)
    }

    @DaoMethod
    Map<Long, String> loadDict(String dictName) {
        return apiMeta().get(ApiMeta).loadDict(dictName)
    }

}
