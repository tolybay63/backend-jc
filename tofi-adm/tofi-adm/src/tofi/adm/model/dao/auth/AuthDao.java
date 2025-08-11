package tofi.adm.model.dao.auth;

import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.store.Store;

import java.util.Map;

public class AuthDao extends BaseModelDao {

    public Map<String, Object> getUserInfo(String l, String p) throws Exception {
        AuthMdbUtils u = new AuthMdbUtils(getMdb());
        return u.getUserInfo(l, p, "adm");
    }

    public Map<String, Object> getCurUserInfo() throws Exception {
        AuthMdbUtils ut = new AuthMdbUtils(getMdb());
        return ut.getCurUserInfo();
    }

    public void regUser(Map<String, Object> params) throws Exception {
        RegMdbUtils ut = new RegMdbUtils(getMdb());
        ut.regUser(params);
    }

    public Store loadProfile(long id) throws Exception {
        AuthMdbUtils ut = new AuthMdbUtils(getMdb());
        return ut.loadProfile(id);
    }

    public void saveProfile(Map<String, Object> rec) throws Exception {
        AuthMdbUtils ut = new AuthMdbUtils(getMdb());
        ut.saveProfile(rec);
    }

    public void savePsw(Map<String, Object> rec) throws Exception {
        AuthMdbUtils ut = new AuthMdbUtils(getMdb());
        ut.savePsw(rec);
    }

/*
    public void checkTarget(String target) throws Exception {
        AuthMdbUtils ut = new AuthMdbUtils(getMdb());
        ut.checkTarget(target);
    }
*/


}
