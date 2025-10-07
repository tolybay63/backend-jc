package dtj.plan.test_obj

import dtj.plan.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store

import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {


    @Test
    void test_findLocationOfCoord() {
/*
        {
			"objWork": 2425,
            "StartKm": 7,
            "FinishKm": 10,
            "StartPicket": 7,
            "FinishPicket": 7
        }
 */

        Map<String, Object> map = Map.of("objWork", 2425, "StartKm", 7,
        "FinishKm", 10, "StartPicket", 7, "FinishPicket", 7) //new HashMap<>()
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.findLocationOfCoord(map)
        mdb.outTable(st)

    }

    @Test
    void testPersonnalInfo() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.getPersonnalInfo(1008)
        mdb.outTable(st)
    }

    @Test
    void testPlanLoad() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadPlan(Map.of(
                "date", "2025-07-29",
                "periodType", 11,
                "objLocation", 1077
        ))
        mdb.outTable(st)
    }

    @Test
    void testCompleteThePlanWork() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.completeThePlanWork(Map.of(
                "id", 1064, "date", "2025-09-12"
        ))
    }

    @Test
    void testPlanSave() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.savePlan("upd", Map.of(
                "id", 1007
        ))
        mdb.outTable(st)
    }

    @Test
    void loadWorkForSelect() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadWorkForSelect(1011)
        mdb.outTable(st)
    }

    @Test
    void loadObjectServedForSelect() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadObjectServedForSelect(0/*2477*/)
        mdb.outTable(st)
    }

    @Test
    void assignPlan_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("", 11)
        map.put("id", 1030)
        map.put("cls", 1132)
        map.put("name", "осмотр 1-18 км [тк №1 - Обходчик обвал. уч. / 435]")
        map.put("idObject", 1338)
        map.put("pvObject", 1076)
        map.put("objObject", 1428)
        map.put("nameObject", "Перегон  [1км 2пк - 19км 1пк] [] [ЖД пути на перегоне] [Шар - Сарыжал]")
        map.put("idUser", 1339)
        map.put("pvUser", 1087)
        map.put("objUser", 1003)
        map.put("nameUser", "Канат С")
        map.put("idLocationClsSection", 3478)
        map.put("pvLocationClsSection", 1241)
        map.put("objLocationClsSection", 1077)
        map.put("nameLocationClsSection", "Околоток № 1")
        map.put("idParameterLog", 1340)
        map.put("pvParameterLog", 1436)
        map.put("objParameterLog", 1271)
        map.put("idStatus", 1337)
        map.put("pvStatus", 1310)
        map.put("fvStatus", 1168)
        map.put("nameStatus", "зарегистрирован")
        map.put("idStartKm", 1341)
        map.put("StartKm", 1.0)
        map.put("idFinishKm", 1342)
        map.put("FinishKm", 2.0)
        map.put("idStartPicket", 1343)
        map.put("StartPicket", 3.0)
        map.put("idFinishPicket", 1344)
        map.put("FinishPicket", 4.0)
        map.put("idStartLink", 1345)
        map.put("StartLink", 5.0)
        map.put("idFinishLink", 1346)
        map.put("FinishLink", 6.0)
        map.put("idDescription", 1350)
        map.put("Description", "Копонент - Рельсо-шпальная решетка / Параметр - ширина колеи, мм, 10.0 (min, 0.0, max, 9.0)")
        map.put("idCreatedAt", 1347)
        map.put("CreatedAt", "2025-07-25")
        map.put("idUpdatedAt", 1348)
        map.put("UpdatedAt", "2025-07-25")
        map.put("idRegistrationDateTime", 1349)
        map.put("RegistrationDateTime", "2025-07-25T10:20:30")
        map.put("AssignDateTime", "2025-07-25T10:20:30")
        map.put("linkCls", 1000)
        map.put("objWork", 2477)
        map.put("pvWork", 1067)
        map.put("PlanDateEnd", "2025-10-30")
        map.put("CreatedAt", "2025-10-20")
        map.put("UpdatedAt", "2025-10-25")

        Long own = dao.assignPlan(map)
        System.out.println(own)
    }

    @Test
    void jsonrpc1() throws Exception {
        Map<String, Object> map = apx.execJsonRpc("api", "data/getPersonnalId", [1013])
        mdb.outMap(map)
    }

}
