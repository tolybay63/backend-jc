package dtj.resource

import dtj.resource.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Material_Test extends Apx_Test {

    @Test
    void loadEquipmentByType_test () {
        DataDao dao = mdb.createDao(DataDao.class)
//        Store st = dao.loadResourceByTyp(1464, "tool", "Prop_Tool")
        Store st = dao.loadResourceByTyp(1460, "equipment", "Prop_Equipment")
        mdb.outTable(st)
    }

    @Test
    void loadResourceByTyp_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceByTyp(1464, "tool", "Prop_Tool")
//        Store st = dao.loadResourceByTyp(1461, "equipment", "Prop_Equipment")
        mdb.outTable(st)
    }

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
        Map <String, Object> map = Map.of(
                "name", "Test 1",
                "Number", "002OOO02",
                "fvTypEquipment", 1253,
                "pvTypEquipment", 1460,
                "Description", "test")
        Store st = dao.saveEquipment("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveEquipment_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = Map.of(
                "id", 1028,
                "cls", 1258,
                "name", "Test 1 upd",
                "idNumber", 1041,
                "Number", "002OOO02",
                "idTypEquipment", 1043,
                "fvTypEquipment", 1254,
                "pvTypEquipment", 1461,
                "idDescription", 1042,
                "Description", "t")
        Store st = dao.saveEquipment("upd", map)
        mdb.outTable(st)
    }

    @Test
    void loadTool_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadTool(0)
        mdb.outTable(st)
    }

    @Test
    void saveTool_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = Map.of(
                "name", "Test 1",
                "fvTypTool", 1257,
                "pvTypTool", 1463,
                "Description", "test")
        Store st = dao.saveTool("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveTool_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = Map.of(
                "id", 1025,
                "name", "Test 2",
                "cls", 1257,
                "idTypTool", 1037,
                "fvTypTool", 1258,
                "pvTypTool", 1464,
                "idDescription", 1036,
                "Description", "t")
        Store st = dao.saveTool("upd", map)
        mdb.outTable(st)
    }

    @Test
    void deleteObjWithProperties_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1000)

    }


}
