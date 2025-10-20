package dtj.repair

import dtj.repair.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Repair_Test extends Apx_Test {

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
        map.put("name", "test 2")
        map.put("objWorkPlan", 1056)
        map.put("pvWorkPlan", 1492)
        map.put("objTask", 3431)
        map.put("pvTask", 1528)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("ValuePlan", 5)
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
        dao.deleteObjWithProperties(0)
    }

}
