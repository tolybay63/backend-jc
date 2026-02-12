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
    }

    public Store loadTypClusterFactor(long id, long typ, String lang) throws Exception {
        Store st = mdb.createStore("TypClusterFactor.lang");
        String whe = "";
        String wheOwn = "";
        if (id > 0) {
            whe = "t.id=" + id;
            wheOwn = " 1 as isOwn ";
        } else {
            TypDao typDao = mdb.createDao(TypDao.class);
            long typParent = typDao.loadRec(Map.of("id", typ, "lang", lang)).get(0).getLong("parent");
            whe = "(t.typ="+typ+" or t.typ="+typParent+")";
            wheOwn = " case when t.typ="+ typ + " then 1 else 0 end as isOwn ";
        }

        mdb.loadQuery(st, """
                  select * from (
                      select t.*, l.name, l.fullName, f.cod, f.ord,
                  """ + wheOwn +
                  """
                      from TypClusterFactor t, Factor f, TableLang l
                      where t.factor=f.id and
                  """ + whe +
                  """
                      and l.nameTable='Factor' and l.idTable=f.id and l.lang=:lang
                  ) a
                  order by isOwn desc, ord
                """, Map.of("lang", lang));

        return st;
    }

    public Store insertTypClusterFactor(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        Store st = mdb.createStore("TypClusterFactor");
        StoreRecord r = st.add(rec);
        //
        long id = mdb.insertRec("TypClusterFactor", r, true);
        //
        return loadTypClusterFactor(id, 0, UtCnv.toString(rec.get("lang")));
    }

    public Store updateTypClusterFactor(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("TypClusterFactor");
        StoreRecord r = st.add(rec);
        //
        mdb.updateRec("TypClusterFactor", r);
        //
        return loadTypClusterFactor(id, 0, UtCnv.toString(params.get("lang")));    }

    public void deleteTypClusterFactor(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("TypClusterFactor", id);
    }

    public Store loadFactors(long typ, String mode, String lang) throws Exception {
        if (mode.equals("ins"))
            return mdb.loadQuery("""
                        select f.id, l.name from Factor f, TableLang l
                        where parent is null and l.idTable=f.id and l.nameTable='Factor' and l.lang=:lang
                            and f.id not in (
                            select factor from TypClusterFactor where typ=:typ
                        )
                        order by ord
                    """, Map.of("typ", typ, "lang", lang));
        else
            return mdb.loadQuery("""
                        select f.id, l.name from Factor f, TableLang l
                        where parent is null and l.idTable=f.id and l.nameTable='Factor' and l.lang=:lang
                        order by ord
                    """, Map.of("typ", typ, "lang", lang));

    }

}
