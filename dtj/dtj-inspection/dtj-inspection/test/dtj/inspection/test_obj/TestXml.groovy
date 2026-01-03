package dtj.inspection.test_obj

import dtj.inspection.dao.ImportXmlDao
import jandcode.core.apx.test.Apx_Test
import org.junit.jupiter.api.Test

class TestXml extends Apx_Test {

    @Test
    void test() {
        //File inputFile = new File("C:\\jc-2\\_info\\xml\\G057_22042025_113706_64_1 1.xml")
        //File inputFile = new File("C:\\jc-2\\_info\\xml\\B057_22042025_113706_1 1.xml")

        File inputFile = new File("C:\\backup\\xml\\G057_22042025_113706_64_1.xml")
//        File inputFile = new File("C:\\backup\\xml\\B057_22042025_113706_1.xml")

        ImportXmlDao dao = mdb.createDao(ImportXmlDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("objUser", 1003)
        map.put("pvUser", 1087)
        map.put("CreatedAt", "2025-12-25")
        map.put("UpdatedAt", "2025-12-25")
        dao.analyze(inputFile, map)
    }
}
