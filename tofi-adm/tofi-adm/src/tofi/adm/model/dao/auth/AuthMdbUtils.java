package tofi.adm.model.dao.auth;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class AuthMdbUtils {
    Mdb mdb;

    public AuthMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
    }

    public Map<String, Object> getUserInfo(String l, String p, String app) throws Exception {
        String p1 = UtString.md5Str(p);
        Store st = mdb.loadQuery("""
            select * from authuser where login=:l and passwd=:p
        """, Map.of("l", l, "p", p1));
        Map<String, Object> res = new HashMap<>();
        if (st.size() == 1) {
            res.putAll(st.get(0).getValues());
            //
            Store stPer = mdb.loadQuery("""
                WITH RECURSIVE r AS (
                   SELECT *
                   FROM permis
                   WHERE parent=:app
                   union ALL
                   SELECT t.*
                   FROM ( SELECT * FROM permis ) t JOIN r ON t.parent = r.id
                ),
                o as (
                    SELECT * FROM permis WHERE id=:app
                ),
                perm as (
                    SELECT * FROM o
                    union ALL
                    SELECT * FROM r where 0=0
                )
                SELECT distinct p.id as permis
                FROM perm p
                    inner join (
                        select distinct * from (
                            select a2.permis
                            from authroleuser a
                                left join authrolepermis a2  on a2.authrole=a.authrole
                            where a.authuser=:u
                            union all
                            select permis
                            from authuserpermis
                            where authuser=:u
                        ) e where 0=0
                    ) t on t.permis=p.id
            """, Map.of("u", UtCnv.toLong(st.get(0).getLong("id")),
                    "app", app));

            Set<Object> sp = stPer.getUniqueValues("permis");
            String target = UtString.join(sp, ",");
            res.put("target", target);
        }

        return res;
    }

    public Map<String, Object> getCurUserInfo() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        if (au==null) {
            throw new XError("NotLogined");
        }
        return au.getAttrs();
    }

    public Store loadProfile(long id) throws Exception {
        Store st = mdb.createStore("AuthUser");
        mdb.loadQuery(st, "select * from AuthUser where id=:id", Map.of("id", id));
        st.get(0).set("passwd", null);
        return st;
    }

    public void saveProfile(Map<String, Object> rec) throws Exception {
        if (rec.size() > 1)
            mdb.updateRec("AuthUser", rec);
    }

    public void savePsw(Map<String, Object> rec) throws Exception {
        String po = UtString.md5Str(UtCnv.toString(rec.get("passwdold")));
        Store st = mdb.loadQuery("select passwd from AuthUser where id=:id",
                Map.of("id", UtCnv.toLong(rec.get("id"))));

        if (po.equals(st.get(0).getString("passwd"))) {
            String p = UtString.md5Str(UtCnv.toString(rec.get("passwd")));
            mdb.updateRec("AuthUser",
                    Map.of("id", UtCnv.toLong(rec.get("id")), "passwd", p));
        } else {
            throw new XError("invalidOldPasswd");
        }
    }

/*
    public void checkTarget(String target) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        long usr = au.getAttrs().getLong("id");
    }
*/

}
