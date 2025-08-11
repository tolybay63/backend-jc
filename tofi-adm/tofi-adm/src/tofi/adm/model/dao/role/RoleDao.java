package tofi.adm.model.dao.role;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class RoleDao extends BaseModelDao {

    public Map<String, Object> loadRolePaginate(Map<String, Object> params) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        return ut.loadRolePaginate(params);
    }

    public void delete(Map<String, Object> params) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        ut.delete(UtCnv.toMap(params.get("rec")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        return ut.update(params);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        return ut.insert(params);
    }

    public Store loadRec(long id) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        return ut.loadRec(id);
    }

    public String getRolePermis(long id) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        return ut.getRolePermis(id);
    }

    public Store loadRolePermis(long role) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        return ut.loadRolePermis(role);
    }

    public Store loadRolePermisForUpd(long role) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        return ut.loadRolePermisForUpd(role);
    }

    public void saveRolePermis(Map<String, Object> params) throws Exception {
        RoleMdbUtils ut = new RoleMdbUtils(getMdb());
        ut.saveRolePermis(params);
    }
}
