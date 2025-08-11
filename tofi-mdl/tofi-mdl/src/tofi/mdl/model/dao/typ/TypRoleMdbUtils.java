package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class TypRoleMdbUtils {
    Mdb mdb;

    public TypRoleMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
        //
/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }

    public Store loadTypRole(long typ) throws Exception {
        Store st = mdb.createStore("TypRole.full");

        mdb.loadQuery(st, """
                  select r.*, v.name, v.fullName from TypRole r, Role v
                  where r.role=v.id and r.typ=:t
                """, Map.of("t", typ));

        return st;
    }

    public Store selectTypRole(long typ) throws Exception {
        return mdb.loadQuery("""
                  select id, name from Role where id not in (select role from TypRole where typ=:t)
                """, Map.of("t", typ));
    }

    public Store insertTypRole(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        Store st = mdb.createStore("TypRole");
        StoreRecord r = st.add(rec);
        //
        long id = mdb.insertRec("TypRole", r, true);

        //
        st = mdb.createStore("TypRole.full");
        mdb.loadQuery(st, """
                  select r.*, v.name, v.fullName from TypRole r, Role v where r.role=v.id and r.id=:id
                """, Map.of("id", id));
        return st;
    }

    public Store updateTypRole(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("TypRole");
        StoreRecord r = st.add(rec);
        //
        mdb.updateRec("TypRole", r);

        //
        st = mdb.createStore("TypRole.full");
        mdb.loadQuery(st, """
                  select r.*, v.name, v.fullName from TypRole r, Role v where r.role=v.id and r.id=:id
                """, Map.of("id", id));
        return st;
    }

    public void deleteTypRole(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.execQuery("delete from TypRoleLifeInterval where typRole=:tr", Map.of("tr", id));
        mdb.deleteRec("TypRole", id);
    }

    /////
    public Store loadTypRoleLife(long typRole) throws Exception {
        Store st = mdb.createStore("TypRoleLifeInterval");
        mdb.loadQuery(st, "select * from TypRoleLifeInterval where typRole=:tr", Map.of("tr", typRole));
        return st;
    }

    public Store insertTypRoleLife(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        Store st = mdb.createStore("TypRoleLifeInterval");
        StoreRecord r = st.add(rec);
        if (r.getString("dbeg").equals("0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (r.getString("dend").equals("0000-01-01"))
            r.set("dend", "3333-12-31");
        long id = mdb.insertRec("TypRoleLifeInterval", r, true);
        //
        st = mdb.createStore("TypRoleLifeInterval");
        mdb.loadQuery(st, """
                  select * from TypRoleLifeInterval where id=:id
                """, Map.of("id", id));
        return st;
    }

    public Store updateTypRoleLife(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("TypRoleLifeInterval");
        StoreRecord r = st.add(rec);
        if (r.getString("dbeg").equals("0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (r.getString("dend").equals("0000-01-01"))
            r.set("dend", "3333-12-31");
        mdb.updateRec("TypRoleLifeInterval", r);
        //
        st = mdb.createStore("TypRoleLifeInterval");
        mdb.loadQuery(st, """
                  select * from TypRoleLifeInterval where id=:id
                """, Map.of("id", id));
        return st;
    }

    public void deleteTypRoleLife(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("TypRoleLifeInterval", id);
    }
}
