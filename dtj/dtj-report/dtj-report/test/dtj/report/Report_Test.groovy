package dtj.report

import dtj.report.dao.ReportDao
import jandcode.core.apx.test.Apx_Test
import org.junit.jupiter.api.Test

class Report_Test extends Apx_Test {

    @Test
    void report1_test () {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = Map.of(
                "dte", "2025-11-04",
                "periodType", 11,
                "objClient", 1014,
                "objLocation", 1077,
                "fulNameUser", "Kanat C.",
                "nameUserPosition", "Тех.отдел",
                "UserPhone", "8-777-666 5544",
                "fullNameDirector", "Мыркинбаев Н.Д.",
                "nameDirectorPosition", "Зам.начальника",
                "nameDirectorLocation", "Досжан Темир Жолы"
        )

        dao.generateReportPO_4(map)
    }

    @Test
    void loadDataPO_4_test () {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = Map.of(
                "dte", "2025-11-04",
                "periodType", 11,
                "objClient", 1014,
                "objLocation", 1077,
                "fulNameUser", "Kanat C.",
                "nameUserPosition", "Тех.отдел",
                "UserPhone", "8-777-666 5544",
                "fullNameDirector", "Мыркинбаев Н.Д.",
                "nameDirectorPosition", "Зам.начальника",
                "nameDirectorLocation", "Досжан Темир Жолы"
        )
        dao.loadDataPO_4(map)
    }
}
