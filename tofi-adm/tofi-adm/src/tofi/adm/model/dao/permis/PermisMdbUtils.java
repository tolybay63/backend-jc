package tofi.adm.model.dao.permis;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreIndex;
import jandcode.core.store.StoreRecord;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class PermisMdbUtils {
    Mdb mdb;

    public PermisMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
    }

    public Store load(String lang) throws Exception {
        Store st = mdb.createStore("Permis.lang");
        String sql = """
            select p.id, p.parent, p.ord, l.fullName as name
            from Permis p
            left join TableLang l on l.nameTable='Permis' and p.id=l.name and l.lang=:lang
            where 0=0
            order by ord
        """;
        return mdb.loadQuery(st, sql, Map.of("lang", lang));
    }

    private void validateRec(String id, String lang) throws Exception {
        Store stTmp = mdb.loadQuery("""
                select l.name
                from AuthRolePermis p
                	left join authrole r on p.authrole=r.id
                	left join TableLang l on l.nameTable='AuthRole' and l.idTable=r.id and l.lang=:lang
                where p.permis=:id
        """, Map.of("id", id, "lang", lang));
        if (stTmp.size() > 0) {
            throw new XError("Используется в роли [{0}]", stTmp.get(0).getString("name"));
        }

        stTmp = mdb.loadQuery("""
            select l.fullname
            from AuthUserPermis p
            left join authuser r on p.authuser=r.id
            left join TableLang l on l.nameTable='AuthUser' and l.idTable=r.id and l.lang=:lang
            where p.permis =:id
        """, Map.of("id", id, "lang", lang));
        if (stTmp.size() > 0) {
            throw new XError("Используется в привилегии пользователя [{0}]", stTmp.get(0).getString("fullname"));
        }
    }

    public void delete(Map<String, Object> rec) throws Exception {
        validateRec(UtCnv.toString(rec.get("id")), UtCnv.toString(rec.get("lang")));

        String sql = """
            delete from Permis where id=:id;
            delete from TableLang where nameTable='Permis' and name=:id;
        """;
        mdb.execQuery(sql, Map.of("id", UtCnv.toString(rec.get("id"))));
    }

    public Store update(Map<String, Object> params) throws Exception {
        String lang = UtCnv.toString(params.get("lang"));
        String sql = """
            update TableLang set fullName=:name where nameTable='Permis' and name=:id and lang=:lang;
        """;
        mdb.execQuery(sql, params);
        //
        return load(lang);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        String sql = """
            insert into Permis (id, parent, ord)
            values (:id, :parent, :ord)
        """;
        int ord = mdb.loadQuery("select max(ord) as max from Permis").get(0).getInt("max");
        params.put("ord", ord+1);
        mdb.execQuery(sql, params);
        //
        StoreRecord rec = mdb.createStoreRecord("TableLang");
        rec.set("nameTable", "Permis");
        rec.set("name", params.get("id"));
        rec.set("fullName", params.get("name"));
        rec.set("lang", params.get("lang"));
        mdb.insertRec("TableLang", rec, true);
        return load(UtCnv.toString(params.get("lang")));
    }

    public Set<String> getLeaf(String id) throws Exception {
        Store st = mdb.createStore("Permis");
        mdb.loadQuery(st, """
            select * from Permis
        """);
        StoreIndex stInd = st.getIndex("id");
        Set<String> leaf = new HashSet<String>();

        StoreRecord record = stInd.get(id);
        while (true) {
            if (record != null) {
                leaf.add(record.getString("id"));
                record = stInd.get(record.getString("parent"));
            } else {
                break;
            }
        }
        return leaf;
    }



}
