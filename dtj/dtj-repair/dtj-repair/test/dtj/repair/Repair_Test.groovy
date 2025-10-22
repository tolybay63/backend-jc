package dtj.repair

import dtj.repair.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Repair_Test extends Apx_Test {


    @Test
    void loadResourceTpService_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceTpService(1042)
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
        Store st = dao.loadResourcePersonnel(1004)
        mdb.outTable(st)
    }

    @Test
    void saveResourcePersonnel_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test 2")
        map.put("objPersonnel", 1006)
        map.put("pvPersonnel", 1538)
        map.put("Value", 1)
        map.put("objTaskLog", 1004)
        map.put("linkCls", 1294)
        map.put("CreatedAt", "2025-10-17")
        map.put("UpdatedAt", "2025-10-17")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourcePersonnel("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourcePersonnel_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1031)
        map.put("cls", 1304)
        map.put("name", "Test upd")
        map.put("idPersonnel", 1142)
        map.put("objPersonnel", 1005)
        map.put("pvPersonnel", 1538)
        map.put("idValue", 1145)
        map.put("Value", 5)
        map.put("idTaskLog", 1143)
        map.put("objTaskLog", 1004)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 1146)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 1147)
        map.put("UpdatedAt", "2025-10-18")
        map.put("idUser", 1144)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourcePersonnel("upd", map)
        mdb.outTable(st)
    }


    @Test
    void loadResourceEquipment_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceEquipment(1004)
        mdb.outTable(st)
    }

    @Test
    void saveResourceEquipment_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test")
        map.put("objEquipment", 1007)
        map.put("pvEquipment", 1536)
        map.put("Value", 1)
        map.put("objTaskLog", 1004)
        map.put("linkCls", 1294)
        map.put("CreatedAt", "2025-10-17")
        map.put("UpdatedAt", "2025-10-17")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceEquipment("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourceEquipment_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1028)
        map.put("cls", 1302)
        map.put("name", "Test upd")
        map.put("idEquipment", 1130)
        map.put("objEquipment", 1007)
        map.put("pvEquipment", 1536)
        map.put("idValue", 1133)
        map.put("Value", 5)
        map.put("idTaskLog", 1131)
        map.put("objTaskLog", 1004)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 1134)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 1135)
        map.put("UpdatedAt", "2025-10-18")
        map.put("idUser", 1132)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceEquipment("upd", map)
        mdb.outTable(st)
    }


    @Test
    void loadResourceTool_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceTool(1004)
        mdb.outTable(st)
    }

    @Test
    void saveResourceTool_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test")
        map.put("objTool", 1003)
        map.put("pvTool", 1535)
        map.put("Value", 1)
        map.put("objTaskLog", 1004)
        map.put("linkCls", 1294)
        map.put("CreatedAt", "2025-10-17")
        map.put("UpdatedAt", "2025-10-17")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceTool("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveResourceTool_upd_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1026)
        map.put("cls", 1301)
        map.put("name", "Test upd")
        map.put("idTool", 1118)
        map.put("objTool", 1003)
        map.put("pvTool", 1535)
        map.put("idValue", 1121)
        map.put("Value", 5)
        map.put("idTaskLog", 1119)
        map.put("objTaskLog", 1004)
        map.put("pvTaskLog", 1533)
        map.put("idCreatedAt", 1122)
        map.put("CreatedAt", "2025-10-17")
        map.put("idUpdatedAt", 1123)
        map.put("UpdatedAt", "2025-10-18")
        map.put("idUser", 1120)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)

        Store st = dao.saveResourceTool("upd", map)
        mdb.outTable(st)
    }

    @Test
    void loadResourceMaterial_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadResourceMaterial(1004)
        mdb.outTable(st)
    }

    @Test
    void saveResourceMaterial_ins_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "Test")
        map.put("objMaterial", 1000)
        map.put("pvMaterial", 1534)
        map.put("meaMeasure", 1037)
        map.put("pvMeasure", 1529)
        map.put("Value", 0)
        map.put("objTaskLog", 1004)
        map.put("linkCls", 1294)
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
        Store st = dao.loadTaskLog(Map.of(
                "date", "2025-07-29",
                "periodType", 11,
                "objLocation", 1071
        ))
        mdb.outTable(st)
    }

    @Test
    void loadTaskLog_test_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadTaskLog_test(0)
        mdb.outTable(st)
    }

    @Test
    void saveTaskLogPlan_ins_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "test new")
        map.put("objWorkPlan", 1056)
        map.put("pvWorkPlan", 1492)
        map.put("objTask", 3431)
        map.put("pvTask", 1528)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("Value", 5)
        map.put("PlanDateStart", "2025-10-17")
        map.put("PlanDateEnd", "2025-10-27")
        map.put("CreatedAt", "2025-10-17")
        map.put("UpdatedAt", "2025-10-17")
        map.put("objLocationClsSection", 1071)
        map.put("pvLocationClsSection", 1275)

        Store st = dao.saveTaskLogPlan("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveTaskLogPlan_upd_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store stTmp = dao.loadTaskLog(1002)
        Map<String, Object> map = stTmp.get(0).getValues()
        map.put("name", "test 2")
        map.put("UpdatedAt", "2025-10-18")

        Store st = dao.saveTaskLogPlan("upd", map)
        mdb.outTable(st)
    }

    @Test
    void saveTaskLogFact_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store stTmp = dao.loadTaskLog(1002)
        Map<String, Object> map = stTmp.get(0).getValues()
        map.put("name", "test 3")
        map.put("FactDateStart", "2025-10-17")

        Store st = dao.saveTaskLogFact(map)
        mdb.outTable(st)
    }

    @Test
    void delete_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1013)
    }

}
