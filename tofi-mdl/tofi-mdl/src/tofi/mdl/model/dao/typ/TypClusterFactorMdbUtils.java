package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class TypClusterFactorMdbUtils {
    Mdb mdb;

    public TypClusterFactorMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
        //
/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }

    public Store loadTypClusterFactor(long typ) throws Exception {
        Store st = mdb.createStore("TypClusterFactor.full");
        TypDao typDao = mdb.createDao(TypDao.class);
        long typParent = typDao.loadRec(typ).getLong("parent");

        mdb.loadQuery(st, """
                  select * from (
                      select t.*, f.name, f.fullName, case when t.typ=:t then 1 else 0 end as isOwn, f.ord
                      from TypClusterFactor t, Factor f
                      where t.factor=f.id and (t.typ=:t or t.typ=:tp)
                  ) a
                  order by isOwn desc, ord
                """, Map.of("t", typ, "tp", typParent));

        return st;
    }

    public Store insertTypClusterFactor(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        Store st = mdb.createStore("TypClusterFactor");
        StoreRecord r = st.add(rec);
        //
        long id = mdb.insertRec("TypClusterFactor", r, true);
        //
        st = mdb.createStore("TypClusterFactor.full");
        mdb.loadQuery(st, """
                  select t.*, f.name, f.fullName, 1 as isOwn from TypClusterFactor t, Factor f
                  where t.factor=f.id and t.id=:id
                """, Map.of("id", id));
        return st;
    }

    public Store updateTypClusterFactor(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("TypClusterFactor");
        StoreRecord r = st.add(rec);
        //
        mdb.updateRec("TypClusterFactor", r);

        //
        st = mdb.createStore("TypClusterFactor.full");
        mdb.loadQuery(st, """
                  select t.*, f.name, f.fullName, 1 as isOwn from TypClusterFactor t, Factor f
                  where t.factor=f.id and t.id=:id
                """, Map.of("id", id));
        return st;
    }

    public void deleteTypClusterFactor(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("TypClusterFactor", id);
    }

    public Store loadFactors(long typ, String mode) throws Exception {
        if (mode.equals("ins"))
            return mdb.loadQuery("""
                        select id, name from Factor where parent is null
                            and id not in (
                            select factor from TypClusterFactor where typ=:typ
                        )
                        order by ord
                    """, Map.of("typ", typ));
        else
            return mdb.loadQuery("""
                        select id, name from Factor where parent is null order by ord
                    """, Map.of("typ", typ));

    }

}
