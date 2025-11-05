package dtj.report

import dtj.report.dao.ReportDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Report_Test extends Apx_Test {

    @Test
    void report1_test() {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("tml", "ПО-4")
        map.put("dte", "2025-11-04")
        map.put("periodType", 11)
        map.put("objClient", 1014)
        map.put("objLocation", 1077)
        map.put("fulNameUser", "Kanat C.")
        map.put("nameUserPosition", "Тех.отдел")
        map.put("UserPhone", "8-777-666 5544")
        map.put("fullNameDirector", "Мыркинбаев Н.Д.")
        map.put("nameDirectorPosition", "Зам.начальника")
        map.put("nameDirectorLocation", "Досжан Темир Жолы")


        dao.generateReportPO_4(map)
    }

    @Test
    void loadDataPO_4_test() {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("tml", "ПО-4")
        map.put("dte", "2025-11-04")
        map.put("periodType", 11)
        map.put("objClient", 1014)
        map.put("objLocation", 1077)
        map.put("fulNameUser", "Kanat C.")
        map.put("nameUserPosition", "Тех.отдел")
        map.put("UserPhone", "8-777-666 5544")
        map.put("fullNameDirector", "Мыркинбаев Н.Д.")
        map.put("nameDirectorPosition", "Зам.начальника")
        map.put("nameDirectorLocation", "Досжан Темир Жолы")
        dao.loadDataPO_4(map)
    }


    @Test
    void jsonrpc1() throws Exception {

/*
        Map<String, Store> map = apx.execJsonRpc("api", "data/loadObj", [1000]) as Map<String, Store>
        mdb.outMap(map)
        map.result.records.forEach {
            mdb.outTable(it)
        }
*/

        apx.execJsonRpc("api", "/loadReport", ["tml", "ПО-4"])

    }


}
