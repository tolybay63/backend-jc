package dtj.object.test_obj

import dtj.object.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {


    @Test
    void test_FV() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadFactorValForSelect("Factor_Periodicity")
        mdb.outTable(st)
    }

    @Test
    void test_Obj() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadObjList("Cls_Collections", "Prop_Collections",  'nsidata')
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
