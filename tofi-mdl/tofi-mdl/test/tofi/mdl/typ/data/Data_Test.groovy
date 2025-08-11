package tofi.mdl.typ.data

import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test
import tofi.api.dta.ApiKPIData
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class Data_Test extends Apx_Test {

    ApinatorApi apiKPIData() {
        return app.bean(ApinatorService).getApi("kpidata")
    }

    @Test
    void test1() {
        Store stObj = apiKPIData().get(ApiKPIData).getObj(1017)

        mdb.outTable(stObj)

    }

}
