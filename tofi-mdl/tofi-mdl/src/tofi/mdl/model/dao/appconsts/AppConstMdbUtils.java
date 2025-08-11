package tofi.mdl.model.dao.appconsts;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.HashMap;
import java.util.Map;

public class AppConstMdbUtils {
    Mdb mdb;

    AppConstMdbUtils(Mdb mdb) {
        this.mdb = mdb;
    }

    public Map<String, StoreRecord> load(Map<String, Object> params) throws Exception {

        Store st = mdb.loadQuery("select * from AppConsts where constname is not null and not constname like '% %' ");

        Map<String, StoreRecord> map = new HashMap<String, StoreRecord>();
        st.forEach(r -> {
            map.put(UtCnv.toString(r.get("constname")), r);
        });

        return map;
    }

}
