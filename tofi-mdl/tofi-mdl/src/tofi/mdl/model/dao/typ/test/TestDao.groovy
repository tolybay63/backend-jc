package tofi.mdl.model.dao.typ.test

import jandcode.core.dbm.dao.BaseModelDao

class TestDao extends BaseModelDao {

    public void importObj(String path, String fn) throws Exception {
        TestMdbUtils ut = new TestMdbUtils(mdb)
        ut.importObj(path, fn)
    }

    public void testFile(Map<String, Object> params) throws Exception {
        TestMdbUtils ut = new TestMdbUtils(mdb)
        ut.testFile(params)
    }

}
