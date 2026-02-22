package tofi.adm.model.dao.role;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.adm.model.utils.UtEntityTranslate;

import java.util.*;

public class RoleMdbUtils {
    Mdb mdb;

    public RoleMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
    }

    public Map<String, Object> loadRolePaginate(Map<String, Object> params) throws Exception {
        String lang = UtCnv.toString(params.get("lang"));
        String filter = UtCnv.toString(params.get("filter")).trim();

        //count
        String sqlCount = """
            select count(*) as cnt
            from AuthRole m
                left join TableLang l on l.nameTable='AuthRole' and m.id=l.idTable and
        """+"l.lang='"+lang+"' where 0=0";

        SqlText sqlText = mdb.createSqlText(sqlCount);
        sqlText.setSql(sqlCount);
        String textFilter = "name like '%" + filter + "%' or fullName like '%" + filter + "%' or cmt like '%" + filter + "%'";
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        int total = mdb.loadQuery(sqlText).get(0).getInt("cnt");
        int lm = UtCnv.toInt(params.get("rowsPerPage")) == 0 ? total : UtCnv.toInt(params.get("rowsPerPage"));
        Map<String, Object> meta = new HashMap<String, Object>();
        meta.put("total", total);
        meta.put("page", UtCnv.toInt(params.get("page")));
        meta.put("limit", lm);

        //query
        String sqlLoad = """
            select *
            from AuthRole m
                left join TableLang l on l.nameTable='AuthRole' and m.id=l.idTable and l.lang=:lang
            where 0=0
            order by m.id
        """;

        sqlText = mdb.createSqlText(sqlLoad);

        int offset = (UtCnv.toInt(params.get("page")) - 1) * lm;
        sqlText.setSql(sqlLoad);
        sqlText.paginate(true);

        if (!UtCnv.toString(params.get("sortBy")).trim().isEmpty()) {
            String orderBy = UtCnv.toString(params.get("sortBy"));
            if (UtCnv.toBoolean(params.get("descending"))) {
                orderBy = orderBy + " desc";
            }
            sqlText = sqlText.replaceOrderBy(orderBy);
        }

        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        //
        Store stLoad = mdb.createStore("AuthRole.lang");
        mdb.loadQuery(stLoad, sqlText, Map.of("lang", lang, "offset", offset, "limit", lm));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        stLoad = ut.getTranslatedStore(stLoad,"AuthRole", lang);
        //
        return Map.of("store", stLoad, "meta", meta);
    }

    public void delete(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        try {
            mdb.deleteRec("AuthRole", id);
        } finally {
            mdb.execQuery("""
                delete from TableLang where nameTable='AuthRole' and idTable=:id
            """, Map.of("id", id));
        }
    }

    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        String lang = UtCnv.toString(rec.get("lang"));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.updateTableLang("AuthRole", rec);
        return loadRec(id, lang);
    }

    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        String lang = UtCnv.toString(rec.get("lang"));
        Store st = mdb.createStore("AuthRole");
        StoreRecord r = st.add(rec);
        long id = mdb.insertRec("AuthRole", r, true);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        rec.put("id", id);
        ut.insertToTableLang("AuthRole", rec);
        //
        return loadRec(id, lang);
    }

    public Store loadRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("AuthRole.lang");
        mdb.loadQuery(st, "select * from AuthRole where id=:id", Map.of("id", id));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "AuthRole", lang);

/*
        return mdb.loadQuery(st, """
            select * from
            AuthRole a
            left join TableLang l on l.nameTable='AuthRole' and l.idTable=:id and l.lang=:lang
            where a.id=:id
        """, Map.of("id", id, "lang", lang));
*/
    }

    public String getRolePermis(long id) throws Exception {

        Store st = mdb.loadQuery("""
                    select p.name from AuthRolePermis r, Permis p where r.authRole=:id and r.permis=p.id
                    order by p.ord
                """, Map.of("id", id));

        Set<Object> set = st.getUniqueValues("name");

        return UtString.join(set, "; ");
    }

    public Store loadRolePermis(long role, String lang) throws Exception {
        Store st = mdb.createStore("Permis.full");
        mdb.loadQuery(st, """
                with a as (
                    select permis, accessLevel from authrolepermis where authrole=:role
                )
                select p.*, a.accessLevel  from permis p, a
                where p.id=a.permis
                order by p.ord
        """, Map.of("role", role));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st, "Permis", lang);

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
