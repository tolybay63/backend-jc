package tofi.api.mdl.utils.dbfilestorage

import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.StoreRecord

class FsDao extends BaseMdbUtils {

    @DaoMethod
    public StoreRecord loadRec(long id) throws Exception {
        StoreRecord rec = mdb.createStoreRecord("DbFileStorage");
        mdb.loadQueryRecord(rec, "select * from DbFileStorage where id=:id", Map.of("id", id));
        return rec;
    }

    @DaoMethod
    public long getNextId() throws Exception {
        return  mdb.getNextId("DbFileStorage");
    }

    @DaoMethod
    public StoreRecord createRecord() throws Exception {
        return mdb.createStoreRecord("DbFileStorage");
    }

    @DaoMethod
    public void ins(StoreRecord rec) throws Exception {
        long id = mdb.insertRec("DbFileStorage", rec);
    }

    @DaoMethod
    public void del(long id) throws Exception {
        mdb.execQuery("""
            delete from DbFileStorage where id=:id
        """, Map.of("id", id));
    }


}
