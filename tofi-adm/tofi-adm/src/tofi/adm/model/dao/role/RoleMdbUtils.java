package tofi.adm.model.dao.role;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.*;

public class RoleMdbUtils {
    Mdb mdb;

    public RoleMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
    }

    public Map<String, Object> loadRolePaginate(Map<String, Object> params) throws Exception {
        String sql = "select * from AuthRole where 0=0 order by id";
        SqlText sqlText = mdb.createSqlText(sql);
        Map<String, Object> par = new HashMap<>();
        int offset = (UtCnv.toInt(params.get("page")) - 1) * UtCnv.toInt(params.get("limit"));
        par.put("offset", offset);
        par.put("limit", UtCnv.toInt(params.get("limit")));
        sqlText.setSql(sql);
        sqlText.paginate(true);

        if (!UtCnv.toString(params.get("orderBy")).trim().isEmpty())
            sqlText = sqlText.replaceOrderBy(UtCnv.toString(params.get("orderBy")));

        String filter = UtCnv.toString(params.get("filter")).trim();
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere("(name like '%" + filter + "%' or fullName like '%" + filter + "%'  or cmt like '%" + filter + "%')");
        Store st = mdb.createStore("AuthRole");
        mdb.loadQuery(st, sqlText, par);
        //count
        sql = "select count(*) as cnt from AuthRole where 0=0";
        sqlText.setSql(sql);
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere("name like '%" + filter + "%' or fullName like '%" + filter + "%' or cmt like '%" + filter + "%'");
        int total = mdb.loadQuery(sqlText).get(0).getInt("cnt");
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("total", total);
        meta.put("page", UtCnv.toInt(params.get("page")));
        meta.put("limit", UtCnv.toInt(params.get("limit")));

        return Map.of("store", st, "meta", meta);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        mdb.deleteRec("AuthRole", UtCnv.toLong(rec.get("id")));
    }

    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        mdb.updateRec("AuthRole", rec);
        //
        Store st = mdb.createStore("AuthRole");
        return mdb.loadQuery(st, "select * from AuthRole where id=:id", Map.of("id",
                rec.get("id")));
    }

    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = mdb.insertRec("AuthRole", rec, true);
        //
        Store st = mdb.createStore("AuthRole");
        mdb.loadQuery(st, "select * from AuthRole where id=:id", Map.of("id", id));
        //
        //mdb.outTable(st);
        return st;
    }

    public Store loadRec(long id) throws Exception {
        Store st = mdb.createStore("AuthRole");
        return mdb.loadQuery(st, """
                    select * from AuthRole where id=:id
                """, Map.of("id", id));
    }

    public String getRolePermis(long id) throws Exception {

        Store st = mdb.loadQuery("""
                    select p.text from AuthRolePermis r, Permis p where r.authRole=:id and r.permis=p.id
                    order by p.ord
                """, Map.of("id", id));

        Set<Object> set = st.getUniqueValues("text");

        return UtString.join(set, "; ");
    }

    public Store loadRolePermis(long role) throws Exception {
        Store st = mdb.createStore("Permis.full");
        return mdb.loadQuery(st, """
                        with a as (
                            select permis, accessLevel from authrolepermis where authrole=:role
                        )
                        select p.*, a.accessLevel  from permis p, a
                        where p.id=a.permis
                        order by p.ord
                """, Map.of("role", role));
    }

    public Store loadRolePermisForUpd(long role) throws Exception {
        Store st = mdb.createStore("Permis.full");
        return mdb.loadQuery(st, """
                    select p.*, a.accessLevel, a.id as idInTable, case when a.id is null then false else true end as checked
                    from permis p
                    left join authrolepermis a on p.id=a.permis and a.authrole=:role
                    order by p.ord
                """, Map.of("role", role));
    }

    public void saveRolePermis(Map<String, Object> params) throws Exception {
        long role = UtCnv.toLong(params.get("role"));
        List<Map<String, Object>> lstData = (List<Map<String, Object>>) params.get("data");

        //Old ids
        Store oldSt = mdb.loadQuery("select id from AuthRolePermis where authRole=:r", Map.of("r", role));
        Set<Object> oldIds = oldSt.getUniqueValues("id");

        //New ids
        Set<Object> newIds = new HashSet<>();
        for (Map<String, Object> map : lstData) {
            newIds.add(UtCnv.toLong(map.get("idInTable")));
        }
        //Deleting
        for (StoreRecord r : oldSt) {
            if (!newIds.contains(r.getLong("id"))) {
                try {
                    mdb.deleteRec("AuthRolePermis", r.getLong("id"));
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
        // Saving
        Store st = mdb.createStore("AuthRolePermis");
        for (Map<String, Object> map : lstData) {
            if (!oldIds.contains(UtCnv.toLong(map.get("idInTable")))) {
                StoreRecord r = st.add(map);
                r.set("id", null);
                r.set("authRole", role);
                r.set("permis", UtCnv.toString(map.get("id")));
                if (UtCnv.toLong(map.get("accessLevel")) > 0)
                    r.set("accessLevel", UtCnv.toLong(map.get("accessLevel")));
                mdb.insertRec("AuthRolePermis", r, true);
            } else {
                StoreRecord r = st.add(map);
                r.set("id", UtCnv.toLong(map.get("idInTable")));
                r.set("authRole", role);
                r.set("permis", UtCnv.toString(map.get("id")));
                r.set("accessLevel", UtCnv.toLong(map.get("accessLevel")));
                mdb.updateRec("AuthRolePermis", r);
            }
        }
    }

}
