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

}
