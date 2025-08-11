package tofi.mdl.model.dao.appconsts;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class AppConstsDao extends BaseModelDao {

    public Map<String, StoreRecord> load(Map<String, Object> params) throws Exception {
        AppConstMdbUtils ut = new AppConstMdbUtils(getMdb());
        return ut.load(params);
    }


}
