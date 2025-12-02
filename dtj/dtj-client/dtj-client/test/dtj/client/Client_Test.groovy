package dtj.client

import dtj.client.dao.DataDao
import jandcode.core.apx.test.Apx_Test
import jandcode.core.store.Store
import org.junit.jupiter.api.Test

class Client_Test extends Apx_Test {

    @Test
    void load_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadClient(0)
        mdb.outTable(st)
    }

    @Test
    void save_ins_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("name", " ")
        map.put("BIN", "1241414494")
        map.put("ContactPerson", "Фамилия И.О")
        map.put("ContactDetails", "г. Астана, ул. 45б, офис 11, тел. 85-858-85")
        map.put("Description", "test 01")

        Store st = dao.saveClient("ins", map)
        mdb.outTable(st)
    }

    @Test
    void loadConstant_test () {
        DataDao dao = mdb.createDao(DataDao.class)
        Store st = dao.loadConstant()
        mdb.outTable(st)
    }

    @Test
    void saveConstant_ins_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1014)
        map.put("FieldDict", "1241414494")
        Store st = dao.saveConstant("ins", map)
        mdb.outTable(st)
    }

    @Test

    void saveConstant_upd_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        Map<String, Object> map = new HashMap<>()
        map.put("id", 1014)
        map.put("idFieldDict", 1039)
        map.put("FieldDict", "1241414494 upd")
        Store st = dao.saveConstant("upd", map)
        mdb.outTable(st)
    }

    @Test
    void delete_test() {
        DataDao dao = mdb.createDao(DataDao.class)
        dao.deleteObjWithProperties(1002)
    }

}
