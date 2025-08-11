package tofi.adm.auth;

import jandcode.core.BaseComp;
import jandcode.core.auth.*;
import jandcode.core.dbm.ModelService;
import jandcode.core.dbm.mdb.Mdb;
import tofi.adm.model.dao.auth.AuthDao;

import java.util.Map;

public class TofiAuthProcessor extends BaseComp implements AuthProcessor {

    public boolean isSupportedAuthToken(AuthToken authToken) {
        return authToken instanceof UserPasswdAuthToken;
    }

    @Override
    public AuthUser login(AuthToken authToken) throws Exception {
        UserPasswdAuthToken token = (UserPasswdAuthToken) authToken;

        ModelService modelSvc = getApp().bean(ModelService.class);
        Mdb mdb =  modelSvc.getModel().createMdb();
        AuthDao dao = mdb.createDao(AuthDao.class);
        Map<String, Object> attrs = dao.getUserInfo(token.getUsername(), token.getPasswd());
        if (attrs.isEmpty()) {
            throw new XErrorAuth(XErrorAuth.msg_invalid_user_passwd);
        }

        AuthUser usr = new DefaultAuthUser(attrs);

        AuthService authSvc = getApp().bean(AuthService.class);
        authSvc.setCurrentUser(usr);
        return usr;

    }


}
