package dtj.object.test_obj

import dtj.object.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {

    @Test
    void loadSection_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadSection(0)
        mdb.outTable(st)
    }

    @Test
    void loadStation_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadStation(0)
        mdb.outTable(st)
    }

    @Test
    void loadStage_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadStage(0)
        mdb.outTable(st)
    }

    @Test
    void saveSection_ins_test() {
        Map<String, Object> map = new HashMap<>()
        map.put("name", "жб мост 3")
        map.put("StartKm", 50)
        map.put("FinishKm", 50)
        map.put("StageLength", 5)
        map.put("objClient", 1014)
        map.put("pvClient", 1320)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("CreatedAt", "2025-01-01")
        map.put("UpdatedAt", "2025-01-02")
        //
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.saveSection("ins", map)
        mdb.outTable(st)
    }

    @Test
    void saveSection_upd_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadSection(12590)
        StoreRecord rec = st.get(0)

        rec.set("name", "жб мост 3 u")
        rec.set("StartKm", 5)
        rec.set("FinishKm", 5)
        rec.set("StageLength", 5)
        rec.set("objClient", 1014)
        rec.set("pvClient", 1320)
        Store stRes = dao.saveSection("upd", rec.getValues())

        mdb.outTable(stRes)
    }

    @Test
    void mappingStAndPer() {
        Map<String, Long> map = new HashMap<>()

        map.put("2635",1876)
        map.put("2636",1875)
        map.put("2637",1874)
        map.put("2638",1873)
        map.put("2639",1872)
        map.put("2640",1871)
        map.put("2641",1870)
        map.put("2643",1877)
        map.put("2645",1863)
        map.put("2646",1864)
        map.put("2647",1865)
        map.put("2648",1866)
        map.put("2649",1867)
        map.put("2650",1868)
        map.put("2651",1869)


        Store st = mdb.loadQuery("""
            select * from DataPropVal where obj in (2635,2636,2637,2638,2639,2643,2645,2646,2647,2648,2649,2650,2651,2640,2641)
        """)

        mdb.outTable(st)
        for (StoreRecord r in st) {
            mdb.execQuery("""
                update DataPropVal set obj=:obj where id=:id
            """, Map.of("obj", map.get(r.getString("obj")), "id", r.getLong("id")))
        }
    }

    @Test
    void test_FV() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadFactorValForSelect("Factor_Periodicity")
        mdb.outTable(st)
    }

    @Test
    void loadObjList_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadObjList("Cls_Event", "Prop_Event",  'incidentdata')
        mdb.outTable(st)
    }

    //****************************
    @Test
    void test_loadObjServed() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadObjectServed(0)
        mdb.outTable(st)
    }

    @Test
    void testSaveObjectServedIns() {
        Map<String, Object> map = new HashMap<>()
        map.put("name", "жб мост 2")
        map.put("fullName", "жб мост 5км 5пк (Мосты) 2")
        map.put("linkCls", 1004)
        map.put("objObjectType", 1025)
        map.put("pvObjectType", 1166)
        map.put("StartKm", 50)
        map.put("FinishKm", 50)
        map.put("StartPicket", 50)
        map.put("FinishPicket", 5)
        map.put("fvSide", 1070)
        map.put("pvSide", 1035)
        map.put("Specs", "жб")
        map.put("LocationDetails", "река Шар")
        map.put("PeriodicityReplacement", 3)
        map.put("Number", "1")
        map.put("InstallationDate", "2022-01-01")
        map.put("CreatedAt", "2025-07-07")
        map.put("UpdatedAt", "2025-07-07")
        map.put("Description", "Железобетонный мост 1")
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("objSection", 1870)
        map.put("pvSection", 1243)
        //
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.saveObjectServed("ins", map)

        mdb.outTable(st)
    }

    @Test
    void testSaveObjectServedUpd() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadObjectServed(1002)
        mdb.outTable(st)
        StoreRecord rec = st.get(0)

        rec.set("name", "жб мост UPD")
        rec.set("UpdatedAt", "2025-07-23")
        rec.set("Description", "Железобетонный мост 1 Update")
        Store stRes = dao.saveObjectServed("upd", rec.getValues())

        mdb.outTable(stRes)
    }

    @Test
    void deleteSaveObjectServed() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1842)
    }

    @Test
    void testFindObj() {
        DataDao dao = mdb.createDao(DataDao.class)
        //Store st = dao.findStationOfCoord(Map.of("StartKm", 2, "FinishKm", 3, "StartPicket", 1, "FinishPicket", 5))
        Store st = dao.findStationOfCoord(Map.of("StartKm", 47, "FinishKm", 47, "StartPicket", 7, "FinishPicket", 7))
        mdb.outTable(st)
    }

    //********************************************************//
    @Test
    void jsonrpc1() throws Exception {
        Map<String, Object> map = apx.execJsonRpc("api", "data/loadObjList", ["Cls_Collections", "Prop_Collections", "nsidata"])
        mdb.outMap(map.get("result") as Map)
    }

}
