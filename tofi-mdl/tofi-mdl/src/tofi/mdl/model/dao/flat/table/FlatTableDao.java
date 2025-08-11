package tofi.mdl.model.dao.flat.table;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class FlatTableDao extends BaseModelDao {

    public Store loadTables(Map<String, Object> params) throws Exception {
        FlatTableMdbUtils ut = new FlatTableMdbUtils(getApp(), getMdb(), "FlatTable");
        return ut.loadTables(params);
    }

    public Store load() throws Exception {
        FlatTableMdbUtils ut = new FlatTableMdbUtils(getApp(), getMdb(), "FlatTable");
        return ut.load();
    }

    public Store loadRec(long id) throws Exception {
        FlatTableMdbUtils ut = new FlatTableMdbUtils(getApp(), getMdb(), "FlatTable");
        return ut.loadRec(id);
    }

    public Store insertFlatTable(Map<String, Object> rec) throws Exception {
        FlatTableMdbUtils ut = new FlatTableMdbUtils(getApp(), getMdb(), "FlatTable");
        return ut.insertFlatTable(rec);
    }

    public Store updateFlatTable(Map<String, Object> rec) throws Exception {
        FlatTableMdbUtils ut = new FlatTableMdbUtils(getApp(), getMdb(), "FlatTable");
        return ut.updateFlatTable(rec);
    }

    public void deleteFlatTable(Map<String, Object> rec) throws Exception {
        FlatTableMdbUtils ut = new FlatTableMdbUtils(getApp(), getMdb(), "FlatTable");
        ut.deleteFlatTable(rec);
    }


}
