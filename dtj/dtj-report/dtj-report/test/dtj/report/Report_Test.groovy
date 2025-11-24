package dtj.report

import dtj.report.dao.ReportDao
import jandcode.commons.UtCnv
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Report_Test extends Apx_Test {

    @Test
    void reportTaskLog_test () {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> params =  Map.of(
//                "notResource", 1,
                "date", "2025-07-29",
                "periodType", 11
        )
        Store st = dao.reportTaskLog(params)
        mdb.outTable(st)
    }

    @Test
    void report_PO_1_test() {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("tml", "ПО-1")
        map.put("date", "2025-11-07")
        map.put("nameLocation", "Location")
        map.put("objClient", 1014)
        map.put("fullNameClient", "DTJ")
        map.put("objLocation", 1073)
        map.put("fulNameUser", "Kanat C.")
        map.put("nameUserPosition", "Тех.отдел")
        map.put("UserPhone", "8-777-666 5544")
        map.put("fullNameDirector", "Мулькибаев Н.Д.")
        map.put("nameDirectorPosition", "Зам.начальника")
        map.put("nameDirectorLocation", "Дистанция пути")
        //
        String res = dao.generateReport(map)
        System.out.println(res)
    }

    @Test
    void report_PO_6_test() {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("tml", "ПО-6")
        map.put("date", "2025-11-07")
        map.put("nameLocation", "Location")
        map.put("objClient", 1014)
        map.put("fullNameClient", "DTJ")
        map.put("objLocation", 1073)
        map.put("fulNameUser", "Kanat C.")
        map.put("nameUserPosition", "Тех.отдел")
        map.put("UserPhone", "8-777-666 5544")
        map.put("fullNameDirector", "Мулькибаев Н.Д.")
        map.put("nameDirectorPosition", "Зам.начальника")
        map.put("nameDirectorLocation", "Досжан Темир Жолы")
        //
        String res = dao.generateReport(map)
        System.out.println(res)
    }

    @Test
    void report_PO_4_test() {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("tml", "ПО-4")
        map.put("date", "2025-11-04")
        map.put("periodType", 11)
        map.put("objClient", 1014)
        map.put("objLocation", 1073)
        map.put("fulNameUser", "Kanat C.")
        map.put("nameUserPosition", "Тех.отдел")
        map.put("UserPhone", "8-777-666 5544")
        map.put("fullNameDirector", "Мыркинбаев Н.Д.")
        map.put("nameDirectorPosition", "Зам.начальника")
        map.put("nameDirectorLocation", "Досжан Темир Жолы")

        String res = dao.generateReport(map)
        System.out.println(res)
    }

    @Test
    void jsonrpc1() throws Exception {
        Map<String, Store> map = apx.execJsonRpc("api", "data/loadObj", [1000]) as Map<String, Store>
        mdb.outMap(map)
        map.result.records.forEach {
            mdb.outTable(it)
        }
    }

    @Test
    void test() {
        File dir = mdb.getApp().appdir + File.separator + "reports" + File.separator as File
        def files = dir.listFiles()

        files.each { file ->
            if (file.isFile()) {
                println file.name + "\t" + file.lastModified()+ "\t" + UtCnv.toDateTime(file.lastModified()) + "\t" + ((new Date()).getTime() - file.lastModified())/1000/60/60

            }
        }


    }


}
