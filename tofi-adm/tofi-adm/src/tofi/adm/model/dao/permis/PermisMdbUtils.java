package tofi.adm.model.dao.permis;

import jandcode.commons.UtCnv;
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

    public Store load(Map<String, Object> params) throws Exception {
        String sql = "select * from Permis where 0=0 order by ord";
        Store st = mdb.createStore("Permis");

        mdb.loadQuery(st, sql);

        return st;
    }

    public void delete(Map<String, Object> rec) throws Exception {
        String sql = """
            delete from Permis where id=:id;
        """;
        mdb.execQuery(sql, Map.of("id", UtCnv.toString(rec.get("id"))));
    }

    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        String sql = """
            update Permis set text=:text where id=:id;
        """;
        mdb.execQuery(sql, rec);
        //
        Store st = mdb.createStore("Permis");
        return mdb.loadQuery(st, "select * from Permis where id=:id", Map.of("id",
                rec.get("id")));
    }

    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        String sql = """
            insert into Permis (id, parent, text, ord)
            values (:id, :parent, :text, :ord)
        """;
        int ord = mdb.loadQuery("select max(ord) as max from Permis").get(0).getInt("max");
        rec.put("ord", ord+1);
        mdb.execQuery(sql, rec);
        //
        Store st = mdb.createStore("Permis");
        mdb.loadQuery(st, "select * from Permis where id=:id", Map.of("id", rec.get("id")));
        //
        //mdb.outTable(st);
        return st;
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
            if (record != null && record.get("parent") != null) {
                leaf.add(record.getString("id"));
                record = stInd.get(record.getString("parent"));
            } else {
                break;
            }
        }
        return leaf;
    }



}
