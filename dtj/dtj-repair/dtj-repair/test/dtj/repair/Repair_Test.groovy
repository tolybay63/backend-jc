package dtj.repair

import dtj.repair.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Repair_Test extends Apx_Test {

    @Test
    void loadResourceAll_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Set<Object> set = new HashSet<>()
        set.add(1223L)
        set.add(1224L)

        Store st = dao.loadResourceAll(set)
        mdb.outTable(st)
    }

    @Test
    void saveResourceFact_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1128)
        map.put("idUser", 1868)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("idValue", 2429)
        map.put("Value", 15)
        map.put("idCreatedAt", 1870)
        map.put("CreatedAt", "2025-10-29")
        dao.saveResourceFact(map)

    }

    @Test
    void loadComplex_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1130)
        Store st = dao.loadComplex(map)
        mdb.outTable(st)
    }


    @Test
    void saveComplex_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1130)
        map.put("objPerformer", 1006)
        map.put("pvPerformer", 1564)
        map.put("PerformerValue", 5)

        Store st = dao.saveComplex("ins", map)
        mdb.outTable(st)
    }

    @Test
    void loadResourceTpService_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceTpService(1126)
        mdb.outTable(st)
    }

    @Test
    void saveResourceTpService_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test 2")
        map.put("objTpService", 1008)
        map.put("pvTpService", 1537)
        map.put("Value", 2)
        //map.put("status", "fact")
        map.put("objTaskLog", 1042)
        map.put("linkCls", 1294)
        map.put("CreatedAt", "2025-10-17")
        map.put("UpdatedAt", "2025-10-17")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceTpService("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourceTpService_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1044)
        map.put("cls", 1303)
        map.put("name", "Test upd")
        map.put("idTpService", 1226)
        map.put("objTpService", 1008)
        map.put("pvTpService", 1537)
        map.put("idValue", 1229)
        map.put("Value", 5)
        map.put("idTaskLog", 1227)
        map.put("objTaskLog", 1042)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 1230)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 1231)
        map.put("UpdatedAt", "2025-10-18")
        map.put("idUser", 1228)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceTpService("upd", map)
        mdb.outTable(st)
    }


    @Test
    void loadResourcePersonnel_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourcePersonnel(1126)
        mdb.outTable(st)
    }

    @Test
    void saveResourcePersonnel_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test 3")
        map.put("fvPosition", 1132)
        map.put("pvPosition", 1248)
        map.put("Value", 1)
        //map.put("status", "fact")
        map.put("Quantity", 1)
        map.put("objTaskLog", 1126)
        map.put("linkCls", 1294)
        map.put("CreatedAt", "2025-10-27")
        map.put("UpdatedAt", "2025-10-27")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourcePersonnel("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourcePersonnel_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1085)
        map.put("cls", 1304)
        map.put("name", "Test upd")
        map.put("idPosition", 1551)
        map.put("fvPosition", 1133)
        map.put("pvPosition", 1249)
        map.put("idValue", 1554)
        map.put("Value", 5)
        map.put("idQuantity", 1557)
        map.put("Quantity", 5)
        map.put("idTaskLog", 1552)
        map.put("objTaskLog", 1004)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 1555)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 1556)
        map.put("UpdatedAt", "2025-10-18")
        map.put("idUser", 1553)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourcePersonnel("upd", map)
        mdb.outTable(st)
    }


    @Test
    void loadResourceEquipment_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceEquipment(1046)
        mdb.outTable(st)
    }

    @Test
    void saveResourceEquipment_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test 2")
        map.put("fvTypEquipment", 1286)
        map.put("pvTypEquipment", 1566)
        map.put("Value", 1)
        //map.put("status", "fact")
        map.put("Quantity", 1)
        map.put("objTaskLog", 1126)
        map.put("linkCls", 1294)
        map.put("CreatedAt", "2025-10-27")
        map.put("UpdatedAt", "2025-10-27")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceEquipment("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourceEquipment_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1113)
        map.put("cls", 1302)
        map.put("name", "Test upd")
        map.put("idTypEquipment", 1763)
        map.put("fvTypEquipment", 1285)
        map.put("pvTypEquipment", 1565)
        map.put("idValue", 1766)
        map.put("Value", 5)
        map.put("idQuantity", 1767)
        map.put("Quantity", 5)
        map.put("idTaskLog", 1764)
        map.put("objTaskLog", 1046)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 1768)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 1769)
        map.put("UpdatedAt", "2025-10-18")
        map.put("idUser", 1765)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceEquipment("upd", map)
        mdb.outTable(st)
    }


    @Test
    void loadResourceTool_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceTool(1126)
        mdb.outTable(st)
    }

    @Test
    void saveResourceTool_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test")
        map.put("fvTypTool", 1289)
        map.put("pvTypTool", 1568)
        map.put("Value", 1)
        //map.put("status", "fact")
        map.put("Quantity", 1)
        map.put("objTaskLog", 1126)
        map.put("linkCls", 1294)
        map.put("CreatedAt", "2025-10-27")
        map.put("UpdatedAt", "2025-10-27")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceTool("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourceTool_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1187)
        map.put("cls", 1301)
        map.put("name", "Test upd")
        map.put("idTypTool", 2167)
        map.put("fvTypTool", 1289)
        map.put("pvTypTool", 1568)
        map.put("idValue", 2170)
        map.put("Value", 5)
        map.put("idQuantity", 2171)
        map.put("Quantity", 5)
        map.put("idTaskLog", 2168)
        map.put("objTaskLog", 1126)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 2172)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 2173)
        map.put("UpdatedAt", "2025-10-18")
        map.put("idUser", 2169)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceTool("upd", map)
        mdb.outTable(st)
    }


    @Test
    void loadResourceMaterial_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceMaterial(1294)
        mdb.outTable(st)
    }

    @Test
    void saveResourceMaterial_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test")
        map.put("objMaterial", 1021)
        map.put("pvMaterial", 1456)
        map.put("meaMeasure", 1004)
        map.put("pvMeasure", 1454)
        map.put("Value", 5)
        map.put("status", "fact")
        map.put("objTaskLog", 1294)
        map.put("linkCls", 1260)
        map.put("CreatedAt", "2025-10-17")
        map.put("UpdatedAt", "2025-10-17")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceMaterial("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourceMaterial_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1013)
        map.put("name", "Test upd")
        map.put("idMaterial", 1075)
        map.put("objMaterial", 1001)
        map.put("pvMaterial", 1534)
        map.put("idMeasure", 1076)
        map.put("meaMeasure", 1037)
        map.put("pvMeasure", 1529)
        map.put("idValue", 1079)
        map.put("Value", 5)
        map.put("idTaskLog", 1077)
        map.put("objTaskLog", 1004)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 1080)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 1081)
        map.put("UpdatedAt", "2025-10-17")
        map.put("idUser", 1078)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceMaterial("upd", map)
        mdb.outTable(st)
    }


    @Test
    void LoadObjClsWorkPlanCorrectionalUnfinishedByDate_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadObjClsWorkPlanCorrectionalUnfinishedByDate(Map.of(
                "date", "2025-10-09",
                "id", 1077,
                "pv", 1241
        ))
        mdb.outTable(st)
    }

    @Test
    void LoadDateWorkPlanCorrectional_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Set<String>  plDate= dao.loadDateWorkPlanCorrectional(["id": 1077, "pv": 1241])
        println(plDate.join(", "))
    }

    @Test
    void loadTaskLogEntriesForWorkPlan_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadTaskLogEntriesForWorkPlan(Map.of(
                "id", 1056,
                "pv", 1492
        ))
        mdb.outTable(st)
    }


    @Test
    void loadTaskLog_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> params =  Map.of(
                "notResource", 1,
                "date", "2025-07-29",
                "periodType", 11,
                "objLocation", 1071
        )
        Map<String, Object> mapRes = dao.loadTaskLog(params)

        mdb.outTable(mapRes.get("store"))

        if (!params.containsKey("notResource")) {
            Store stResource = mapRes.get("resource") as Store
            mdb.outTable(stResource)
        }

    }

    @Test
    void saveTaskLogPlan_ins_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "test new")
        map.put("objWorkPlan", 1289)
        map.put("pvWorkPlan", 1442)
        map.put("objTask", 3398)
        map.put("pvTask", 1467)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("Value", 5)
        map.put("PlanDateStart", "2025-11-17")
        map.put("PlanDateEnd", "2025-17-27")
        map.put("CreatedAt", "2025-11-18")
        map.put("UpdatedAt", "2025-11-18")
        map.put("objLocationClsSection", 1084)
        map.put("pvLocationClsSection", 1241)

        Map<String, Object> st = dao.saveTaskLogPlan("ins", map)
        mdb.outTable(st.get("store"))
    }

    @Test
    void saveTaskLogPlan_upd_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> mapRez = dao.loadTaskLog(Map.of("id", 1095))
        Store stTmp = mapRez.get("store") as Store
        Map<String, Object> map = stTmp.get(0).getValues()
        map.put("name", "test 2")
        map.put("UpdatedAt", "2025-10-18")

        Map<String, Object> st = dao.saveTaskLogPlan("upd", map)
        mdb.outTable(st.get("store"))
    }


    @Test
    void loadTaskLog_test_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadTaskLog_test(0)
        mdb.outTable(st)
    }

    @Test
    void saveTaskLogFact_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store stTmp = dao.loadTaskLog(Map.of("id", 1002)).get("store") as Store
        Map<String, Object> map = stTmp.get(0).getValues()
        map.put("name", "test 3")
        map.put("FactDateStart", "2025-10-17")

        Map<String, Object> mapRes = dao.saveTaskLogFact(map)
        mdb.outMap(mapRes)
    }

    @Test
    void delete_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1013)
    }

}
