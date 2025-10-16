package dtj.resource

import dtj.resource.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Material_Test extends Apx_Test {

    @Test
    void load_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadMaterial(0)
        mdb.outTable(st)
    }

    @Test
    void save_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = Map.of("name", "цемент", "meaMeasure", 1036, "pvMeasure", 1508)
        Store st = dao.saveMaterial("ins", map)
        mdb.outTable(st)
    }

    @Test
    void save_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = Map.of("id", 1000, "cls", 1289, "name", "цемент",
                "idMeasure", 1000, "meaMeasure", 1010, "pvMeasure", 1323)
        Store st = dao.saveMaterial("upd", map)
        mdb.outTable(st)
    }


}
