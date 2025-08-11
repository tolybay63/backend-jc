package tofi.mdl.model.dao.auth;

import jandcode.commons.error.XError;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dao.DaoMethod;
import jandcode.core.dbm.mdb.BaseMdbUtils;

import java.util.Map;

public class AuthDao extends BaseMdbUtils {

    @DaoMethod
    public Map<String, Object> getCurUserInfo() {
        AuthService authSvc = getMdb().getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        if (au==null) {
            throw new XError("NotLogined");
        }
        return au.getAttrs();
    }

}
