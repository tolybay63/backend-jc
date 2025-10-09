package dtj.incident.test_obj

import dtj.incident.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Obj_Test extends Apx_Test {

    @Test
    void test_loadEvent() {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadEvent(0)
        mdb.outTable(st)
    }

    @Test
    void test_saveEvent_ins() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> rec = new HashMap<>()
        //
        rec.put("name", "test Event")
        //
        Store st = dao.saveEvent("ins", rec)
        mdb.outTable(st)
    }

    @Test
    void delete_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1004)
    }

    @Test
    void test_loadIncident() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("periodType", 11)
        map.put("date", "2025-09-09")
        map.put("objLocation", 1071)
        Store st = dao.loadIncident(map)
        mdb.outTable(st)
    }

    @Test
    void test_saveIncident_ins() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", "test03 for datetime ********")
        map.put("codCls", "Cls_IncidentContactCenter")
        map.put("objEvent", 1001)
        map.put("pvEvent", 1434)
        map.put("objObject", 1068)
        map.put("pvObject", 1075)
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
//        map.put("objParameterLog", )
//        map.put("pvParameterLog", )
//        map.put("objFault", )
//        map.put("pvFault", )
        map.put("StartKm", 1)
        map.put("FinishKm", 2)
        map.put("StartPicket", 3)
        map.put("FinishPicket", 4)
        map.put("StartLink", 5)
        map.put("FinishLink", 6)
        map.put("CreatedAt",  "2025-09-26")
        map.put("UpdatedAt",  "2025-09-26")
        map.put("RegistrationDateTime",  "2025-09-26T10:20:30.000")
        map.put("Description", "test02 **********")
        map.put("InfoApplicant", "Kazybek **********")

        Store st = dao.saveIncident("ins", map)
        mdb.outTable(st)

    }

    @Test
    void test_saveIncident_upd() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1049)
        map.put("idCriticality", 1606)
        map.put("pvCriticality", 1319)
        map.put("fvCriticality", 1178)
        map.put("idInfoApplicant", 1618)
        map.put("idDescription", 1617)
        map.put("idUpdatedAt", 1615)
        map.put("UpdatedAt",  "2025-10-26")
        map.put("Description", "test02 **********")
        map.put("InfoApplicant", "Kazybek **********")

        Store st = dao.saveIncident("upd", map)
        mdb.outTable(st)

    }


}
