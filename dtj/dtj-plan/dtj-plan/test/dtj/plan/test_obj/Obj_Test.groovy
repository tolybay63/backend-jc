package dtj.plan.test_obj

import dtj.plan.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store

import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {

    @Test
    void loadSizePlanOfMonth_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        double d = dao.loadSizePlanOfMonth(Map.of(
                "date", "2025-11-10",
//                "periodType", 71,
                "objLocation", 1073,
                "codCls", "Cls_WorkPlanCorrectional"
        ))

        System.println(d)
    }

    @Test
    void loadPlanCorrectional_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadPlanCorrectional(Map.of(
                "date", "2025-07-29",
                "periodType", 11,
                "objLocation", 1077
        ))
        mdb.outTable(st)
    }


    @Test
    void test_loadWorkOnObjectServedForSelect() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadWorkOnObjectServedForSelect(0)
        mdb.outTable(st)
    }

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

        Map<String, Object> map = Map.of("objWork", 0, "StartKm", 7,
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
                "date", "2025-11-17",
                "periodType", 41,
                "objLocation", 1073
//                "codCls", "Cls_WorkPlanCorrectional"
        ))
        mdb.outTable(st)
    }

    @Test
    void testCompleteThePlanWork() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.completeThePlanWork(Map.of(
                "id", 1290, "cls", 1063,"date", "2025-11-19"
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
        Store st = dao.loadObjectServedForSelect(2477)
        mdb.outTable(st)
    }

    @Test
    void assignPlan_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()

        map.put("CreatedAt", "2025-11-17")
        map.put("FinishKm", 19)
        map.put("FinishPicket", 2)
        map.put("PlanDateEnd", "2025-11-25")
        map.put("StartKm", 19)
        map.put("StartPicket", 2)
        map.put("UpdatedAt", "2025-11-17")
        map.put("cls", 1134)
        map.put("fvCriticality", 1174)
        map.put("id", 1158)
        map.put("idStatus", 3064)
        map.put("linkCls", 1002)
        map.put("name", "1107-2025-11-17")
        map.put("objLocationClsSection", 1077)
        map.put("objObject", 1521)
        map.put("objUser", 1003)
        map.put("objWork", 2345)
        map.put("pvCriticality", 1315)
        map.put("pvLocationClsSection", 1241)
        map.put("pvObject", 1082)
        map.put("pvUser", 1087)
        map.put("pvWork", 1069)
        map.put("AssignDateTime", "2025-11-17T00:00:00.000")

        Long own = dao.assignPlan(map)
        System.out.println(own)
    }

    @Test
    void jsonrpc1() throws Exception {
        Map<String, Object> map = apx.execJsonRpc("api", "data/getPersonnalId", [1013])
        mdb.outMap(map)
    }

}
