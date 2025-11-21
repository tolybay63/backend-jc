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
                "objUser", 1003,
                "pvUser", 1087,
                "objLocationClsSection", 1077,
                "pvLocationClsSection", 1241,
                "CreatedAt", "2025-11-18",
                "UpdatedAt", "2025-11-18")
        Store st = dao.saveEquipment("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveEquipment_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = new HashMap<>()
        map.put("id", 1183)
        map.put("name", "Test 3")
        map.put("cls", 1258)
        map.put("idTypEquipment", 1295)
        map.put("fvTypEquipment", 1258)
        map.put("pvTypEquipment", 1460)
        map.put("idNumber", 1294)
        map.put("Number", 1265)
        map.put("idLocationClsSection", 1248)
        map.put("objLocationClsSection", 1077)
        map.put("pvLocationClsSection", 1241)
        map.put("idUpdatedAt", 1250)
        map.put("UpdatedAt", "2025-11-18")
        map.put("idUser", 1251)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
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
                "Number", "1265",
                "fvTypTool", 1265,
                "pvTypTool", 1482,
                "objUser", 1003,
                "pvUser", 1087,
                "objLocationClsSection", 1077,
                "pvLocationClsSection", 1241,
                "CreatedAt", "2025-11-18",
                "UpdatedAt", "2025-11-18")
        Store st = dao.saveTool("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveTool_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map <String, Object> map = new HashMap<>()
                map.put("id", 1181)
                map.put("name", "Test 2")
                map.put("cls", 1257)
                map.put("idTypTool", 1287)
                map.put("fvTypTool", 1258)
                map.put("pvTypTool", 1464)
                map.put("idNumber", 1286)
                map.put("Number", 1265)
                map.put("idLocationClsSection", 1288)
                map.put("objLocationClsSection", 1077)
                map.put("pvLocationClsSection", 1241)
                map.put("idUpdatedAt", 1290)
                map.put("UpdatedAt", "2025-11-18")
                map.put("idUser", 1291)
                map.put("objUser", 1003)
                map.put("pvUser", 1087)
        Store st = dao.saveTool("upd", map)
        mdb.outTable(st)
    }

    @Test
    void deleteObjWithProperties_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1183)

    }


}
