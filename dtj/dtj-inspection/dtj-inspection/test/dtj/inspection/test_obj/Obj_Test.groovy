package dtj.inspection.test_obj

import dtj.inspection.dao.DataDao
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
    void testSaveInspection() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "test01")
        map.put("objLocationClsSection", 1077)
        map.put("pvLocationClsSection", 1241)
        map.put("objWorkPlan", 1007)
        map.put("pvWorkPlan", 1286)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("StartKm", 1)
        map.put("FinishKm", 2)
        map.put("StartPicket", 3)
        map.put("FinishPicket", 4)
        map.put("StartLink", 5)
        map.put("FinishLink", 6)
        map.put("FactDateEnd", "2025-08-01")
        map.put("CreatedAt", "2025-08-11")
        map.put("UpdatedAt", "2025-08-11")
        map.put("ReasonDeviation", "test02")
        map.put("fvDeviationDefect", 1074)
        map.put("pvDeviationDefect", 1289)


        Store st = dao.saveInspection("ins", map)
        mdb.outTable(st)
    }


    @Test
    void testLoadInspection() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadInspection(Map.of(
                "date", "2025-07-29",
                "periodType", 11,
                "objLocation", 1077
        ))
        mdb.outTable(st)
    }

    @Test
    void deleteInspection() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1001)
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
