package tofi.adm.model.dao.auth;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.core.dbm.dao.BaseModelDao;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class RegMdbUtils extends BaseModelDao {
    Mdb mdb;

    public RegMdbUtils(Mdb mdb) {
        this.mdb = mdb;
    }


    public void regUser(Map<String, Object> rec) throws Exception {
        String psw = UtString.md5Str(UtCnv.toString(rec.get("passwd")));
        String login = UtString.toString(rec.get("login")).trim();
        Store st = mdb.loadQuery("""
                    select id from AuthUser where login like :l
                """, Map.of("l", login));
        if (st.size() > 0) {
            throw new XError("loginExists");
        }

        rec.put("passwd", psw);

        //
        st = mdb.createStore("AuthUser");
        StoreRecord r = st.add(rec);
        r.set("authUserGr", 2);
        r.set("locked", 0);
        mdb.insertRec("AuthUser", r, true);
        //
        if (getApp().getEnv().getProperties().getString("keycloak").equals("true")) {
            var kc = new tofi.adm.auth.KeycloakAdminClient(getMdb().getApp());
            //создать пользователя в Keycloak
            String kcUserId = kc.createUser(r.getString("login"), r.getString("name"),
                    r.getString("fullName"), r.getString("email"), true);
            //задать пароль (не временный)
            kc.setUserPassword(kcUserId, r.getString("passwd"), false);
        }
        //


    }



}
