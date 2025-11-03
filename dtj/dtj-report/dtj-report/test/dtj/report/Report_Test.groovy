package dtj.report

import dtj.report.dao.ReportDao
import jandcode.core.apx.test.Apx_Test
import org.junit.jupiter.api.Test

class Report_Test extends Apx_Test {

    @Test
    void report1_test () {
        ReportDao dao = mdb.createDao(ReportDao.class)
        Map<String, Object> map = Map.of(
                "tml", "ПО-4")


        dao.loadFile(map)


    }




}
