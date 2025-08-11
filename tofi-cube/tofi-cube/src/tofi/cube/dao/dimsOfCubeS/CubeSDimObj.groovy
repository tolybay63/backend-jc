package tofi.cube.dao.dimsOfCubeS

import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class CubeSDimObj extends BaseMdbUtils {
    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    Mdb mdb
    private long cubeS
    private Set<Long> setDimObj

    CubeSDimObj(Mdb mdb, long cubeSId, Map<String, Set<Long>> mapDimObj) {
        this.mdb = mdb
        this.cubeS = cubeSId
        this.setDimObj = mapDimObj["dimObj"]
    }

    void createTable() {
        //todo...

    }

    void fillTable() {
        //todo...

    }

    //
    private Store loadSqlMeta(String sql, String domain) {
        return apiMeta().get(ApiMeta).loadSql(sql, domain)
    }




}
