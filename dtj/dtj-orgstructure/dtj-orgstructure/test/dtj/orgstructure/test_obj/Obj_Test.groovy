package dtj.orgstructure.test_obj

import dtj.orgstructure.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {

    @Test
    void loadLocation() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadLocation(0)
        mdb.outTable(st)
    }

    @Test
    void saveLocation() {
        Map<String, Object> map = new HashMap<>()
        map.put("id",1027)
        map.put("cls",1006)
        map.put("name","Путевые знаки")
        map.put("pv",1197)
        List<Map> lst = new ArrayList<>()
        lst.add(map)

        Map<String, Object> map2 = new HashMap<>()
        map2.put("id", 1094)
        map2.put("cls", 1070)
        map2.put("name", "test")
        map2.put("objObjectTypeMulti", lst)

        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.saveLocation("upd", map2)
        mdb.outTable(st)
    }


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
        dao.deleteObjWithProperties(1094)
    }

}
