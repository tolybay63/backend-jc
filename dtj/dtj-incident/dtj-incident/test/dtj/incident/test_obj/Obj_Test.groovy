package dtj.incident.test_obj

import dtj.incident.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {

    @Test
    void test_loadEvent() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadEvent(0)
        mdb.outTable(st)
    }

    @Test
    void test_saveEvent_ins() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> rec = new HashMap<>()
        //
        rec.put("name", "test Event")
        rec.put("fvCriticality", 1178)
        rec.put("pvCriticality", 1319)
        //
        Store st = dao.saveEvent("ins", rec)
        mdb.outTable(st)
    }

    @Test
    void delete_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1000)
    }


}
