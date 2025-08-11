package tofi.cube.dao.dimsOfCubeS


import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class CubeSDimProp extends BaseMdbUtils {

    ApinatorApi apiMetaCube() {
        return app.bean(ApinatorService).getApi("meta")
    }

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    Mdb mdb
    private long cubeS

    private Set<Long> setDimProp
    private Set<Long> setDimPropF
    private Set<Long> setDimPropML
    private Set<Long> setDimPropMP

    CubeSDimProp(Mdb mdb, long cubeSId, Map<String, Set<Long>> mapDimProps) {
        this.mdb = mdb
        this.cubeS = cubeSId
        this.setDimProp = mapDimProps["dimProp"]
        this.setDimPropF = mapDimProps["dimPropF"]
        this.setDimPropML = mapDimProps["dimPropML"]
        this.setDimPropMP = mapDimProps["dimPropMP"]
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
