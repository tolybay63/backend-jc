package dtj.inspection.auth;

import jandcode.core.BaseComp;
import jandcode.core.auth.*;
import tofi.api.adm.ApiAdm;
import tofi.apinator.ApinatorService;

public class TofiAuthProcessor extends BaseComp implements AuthProcessor {

    public boolean isSupportedAuthToken(AuthToken authToken) {
        return authToken instanceof UserPasswdAuthToken;
    }

    @Override
    public AuthUser login(AuthToken authToken) throws Exception {
        UserPasswdAuthToken token = (UserPasswdAuthToken) authToken;

        var admApi = getApp().bean(ApinatorService.class).getApi("adm");
        var z = admApi.get(ApiAdm.class);
        var attrs = z.getUserInfo(token.getUsername(), token.getPasswd(), "dtj");


        if (attrs.isEmpty()) {
            throw new XErrorAuth(XErrorAuth.msg_invalid_user_passwd);
        }

        AuthUser usr = new DefaultAuthUser(attrs);

        AuthService authSvc = getApp().bean(AuthService.class);
        authSvc.setCurrentUser(usr);
        return usr;

    }


}
