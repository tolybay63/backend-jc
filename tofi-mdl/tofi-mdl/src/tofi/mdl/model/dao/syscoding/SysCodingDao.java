package tofi.mdl.model.dao.syscoding;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class SysCodingDao extends BaseModelDao {


    public Store load(String lang) throws Exception {
        SysCodingMdbUtils utils = new SysCodingMdbUtils(getMdb(), "SysCoding");
        return utils.load(lang);
    }

    public StoreRecord newRec() throws Exception {
        SysCodingMdbUtils utils = new SysCodingMdbUtils(getMdb(), "SysCoding");
        return utils.newRec();
    }

    public Store insert(Map<String, Object> rec) throws Exception {
        SysCodingMdbUtils utils = new SysCodingMdbUtils(getMdb(), "SysCoding");
        return utils.insert(rec);
    }

    public Store update(Map<String, Object> rec) throws Exception {
        SysCodingMdbUtils utils = new SysCodingMdbUtils(getMdb(), "SysCoding");
        return utils.update(rec);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        SysCodingMdbUtils utils = new SysCodingMdbUtils(getMdb(), "SysCoding");
        utils.delete(rec);
    }



}
