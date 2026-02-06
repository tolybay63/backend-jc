package tofi.mdl.model.dao.role;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class RoleDao extends BaseModelDao {

    public Store load(Map<String, Object> params) throws Exception {
        RoleMdbUtils u = new RoleMdbUtils(getMdb(), "Role");
        return u.load(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        RoleMdbUtils u = new RoleMdbUtils(getMdb(), "Role");
        u.delete(UtCnv.toMap(params.get("rec")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        RoleMdbUtils u = new RoleMdbUtils(getMdb(), "Role");
        return u.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        RoleMdbUtils u = new RoleMdbUtils(getMdb(), "Role");
        return u.insert(params);
    }

    public StoreRecord loadRec(Map<String, Object> params) throws Exception {
        RoleMdbUtils u = new RoleMdbUtils(getMdb(), "Role");
        return u.loadRec(params);
    }

    public Store loadRoles(String lang) throws Exception {
        RoleMdbUtils u = new RoleMdbUtils(getMdb(), "Role");
        return u.loadRoles(lang);
    }



}
