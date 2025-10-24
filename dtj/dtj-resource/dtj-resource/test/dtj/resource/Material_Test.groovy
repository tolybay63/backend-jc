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
                "idMeasure", 1000, /*"meaMeasure", 1010,*/ "pvMeasure", 1323)
        Store st = dao.saveMaterial("upd", map)
        mdb.outTable(st)
    }

    @Test
    void loadEquipment_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadEquipment(0)
        mdb.outTable(st)
    }

    @Test
    void saveEquipment_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = Map.of("name", "Техника 2", "Number", "002OOO01")
        Store st = dao.saveEquipment("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveEquipment_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = Map.of("name", "Газель", "idNumber", "1015", "Number", "0001",
                "id", 1014, "cls", 1291, "idDescription", 1016)
        Store st = dao.saveEquipment("upd", map)
        mdb.outTable(st)
    }

    @Test
    void deleteObjWithProperties_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1000)

    }


}
