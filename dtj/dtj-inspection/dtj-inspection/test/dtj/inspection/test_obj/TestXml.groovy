package dtj.inspection.test_obj

import dtj.inspection.dao.ImportDao
import jandcode.core.apx.test.Apx_Test
import org.junit.jupiter.api.Test

class TestXml extends Apx_Test {

    @Test
    void test() {
        //File inputFile = new File("C:\\jc-2\\_info\\xml\\G057_22042025_113706_64_1 1.xml")
        File inputFile = new File("C:\\jc-2\\_info\\xml\\B057_22042025_113706_1 1.xml")
        ImportDao dao = mdb.createDao(ImportDao.class)
        dao.analyze(inputFile)
    }
}
