package dtj.repair

import dtj.repair.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Repair_Test extends Apx_Test {

    @Test
    void loadTaskLog_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadTaskLog(0)
        mdb.outTable(st)
    }

    @Test
    void saveTaskLogPlan_ins_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "test 1")
        map.put("objWorkPlan", 1036)
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
