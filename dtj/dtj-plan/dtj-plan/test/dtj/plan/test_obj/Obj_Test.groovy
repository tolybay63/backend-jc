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
        Store st = dao.getPersonnalInfo(1013)
        mdb.outTable(st)
    }

    @Test
    void testPlanLoad() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadPlan(Map.of(
                "date", "2025-07-29",
                "periodType", 11,
                "objLocation", 1010
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
    void jsonrpc1() throws Exception {
        Map<String, Object> map = apx.execJsonRpc("api", "data/getPersonnalId", [1013])
        mdb.outMap(map)
    }

}
