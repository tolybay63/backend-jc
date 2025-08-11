package dtj.orgstructure.test_obj

import dtj.orgstructure.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {

    @Test
    void jsonrpc1() throws Exception {
        Map<String, Object> map = apx.execJsonRpc("api", "data/loadObjList", ["Cls_WorkCheckInspect", "nsidata"])
        mdb.outMap(map.get("result") as Map)
    }


    @Test
    void test_fv() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadFactorValForSelect("Prop_Region")
        mdb.outTable(st)
    }

    @Test
    void delectLocation() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1003)
    }

}
