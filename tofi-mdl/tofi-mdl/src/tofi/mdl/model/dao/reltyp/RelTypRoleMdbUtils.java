package tofi.mdl.model.dao.reltyp;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RelTypRoleMdbUtils {
    Mdb mdb;

    public RelTypRoleMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
    }

    public Store loadRelTypRole(long id, long reltyp, String lang) throws Exception {
        String whe = "t.reltyp=:reltyp";
        if (id > 0) {
            whe = "t.id=:id";
        }

        Store stTmp = mdb.loadQuery("""
           select role from RelTypRole where reltyp="""+reltyp);
        Set<Object> idsRole = stTmp.getUniqueValues("role");
        String ids = UtString.join(idsRole, ",");


        //Перевод Role
        Store st = mdb.loadQuery("""
          select r.*, l.name, l.fullName, l.cmt, l.idTable
          from Role r
            left join TableLang l on l.nameTable='Role' and l.idTable=r.id and l.lang=:lang
          where r.id in (0"""+ids+")", Map.of("lang", lang));

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.getTranslatedStore(st,"Role", lang);

        //Перевод RelTypRole
        Store stTypRole = mdb.createStore("RelTypRole.lang");
        mdb.loadQuery(stTypRole, """
          select t.*, l2.cmt
          from RelTypRole t
            left join TableLang l2 on l2.nameTable='RelTypRole' and l2.idTable=t.id and l2.lang=:lang
          where
        """+whe, Map.of("id", id, "reltyp", reltyp, "lang", lang));
        ut.getTranslatedStore(stTypRole,"RelTypRole", lang);
        //

        //Загрузка RelTypRole: name,fullName from Role, cmt from RelTypeRole
        Store stLoad = mdb.createStore("RelTypRole.full");
        mdb.loadQuery(stLoad, """
          select t.*, l.name, l.fullName, l2.cmt
          from RelTypRole t
            left join Role r on t.role = r.id
            left join TableLang l on l.nameTable='Role' and l.idTable=r.id and l.lang=:lang
            left join TableLang l2 on l2.nameTable='RelTypRole' and l2.idTable=t.id and l2.lang=:lang
          where
        """+whe, Map.of("id", id, "reltyp", reltyp, "lang", lang));

        return stLoad;
    }

    public Store selectRelTypRole(long reltyp, String lang) throws Exception {
        Store st = mdb.createStore("Role.lang");
        mdb.loadQuery(st,"""
          select t.id, l2.name, l2.cmt
          from Role t
            left join TableLang l2 on l2.nameTable='Role' and l2.idTable=t.id and l2.lang=:lang
          where t.id not in (select role from RelTypRole where reltyp=:rt)
        """, Map.of("lang", lang, "rt", reltyp));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Role", lang);
    }

    public Store insertRelTypRole(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("RelTypRole");
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        StoreRecord r = st.add(rec);
        //
        long id = mdb.insertRec("RelTypRole", r, true);
        //
        Map<String, Object> map = new HashMap<>();
        map.put("table", "RelTypRole");
        map.put("id", id);
        map.put("lang", UtCnv.toString(rec.get("lang")));
        map.put("cmt", UtCnv.toString(rec.get("cmt")));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.insertToTableLang(map);

        return loadRelTypRole(id, 0, UtCnv.toString(rec.get("lang")));
    }

    public Store updateRelTypRole(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("RelTypRole");
        StoreRecord r = st.add(rec);
        //
        mdb.updateRec("RelTypRole", r);
        //
        return loadRelTypRole(id, 0, UtCnv.toString(rec.get("lang")));
    }

    public void deleteRelTypRole(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.execQuery("""
            delete from TableLang
            where nameTable='RelTypRoleLifeInterval' and
                idTable in (select id from RelTypRoleLifeInterval where reltypRole=:tr);
            delete from TypRoleLifeInterval where reltypRole=:tr
        """, Map.of("tr", id));
        mdb.deleteRec("RelTypRole", id);
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.deleteFromTableLang("RelTypRole", id);
    }

    /////
    public Store loadRelTypRoleLife(long id, long reltypRole, String lang) throws Exception {
        Store st = mdb.createStore("RelTypRoleLifeInterval.lang");
        String whe = "reltypRole=:tr";
        if (id>0)
            whe = "id=:id";
        mdb.loadQuery(st, "select * from RelTypRoleLifeInterval where "+whe, Map.of("id", id, "tr", reltypRole));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        //
        return ut.getTranslatedStore(st,"RelTypRoleLifeInterval", lang);
    }

    public Store insertRelTypRoleLife(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        Store st = mdb.createStore("RelTypRoleLifeInterval");
        StoreRecord r = st.add(rec);
        if (r.getString("dbeg").equals("0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (r.getString("dend").equals("0000-01-01"))
            r.set("dend", "3333-12-31");
        long id = mdb.insertRec("RelTypRoleLifeInterval", r, true);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        Map<String, Object> map = new HashMap<>();
        map.put("table", "RelTypRoleLifeInterval");
        map.put("id", id);
        map.put("lang", UtCnv.toString(rec.get("lang")));
        map.put("cmt", UtCnv.toString(rec.get("cmt")));
        ut.insertToTableLang(map);
        //
        return loadRelTypRoleLife(id, 0, UtCnv.toString(rec.get("lang")));
    }

    public Store updateRelTypRoleLife(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("RelTypRoleLifeInterval");
        StoreRecord r = st.add(rec);
        if (r.getString("dbeg").equals("0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (r.getString("dend").equals("0000-01-01"))
            r.set("dend", "3333-12-31");
        mdb.updateRec("RelTypRoleLifeInterval", r);
        //
        return loadRelTypRoleLife(id, 0, UtCnv.toString(rec.get("lang")));
    }

    public void deleteRelTypRoleLife(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("RelTypRoleLifeInterval", id);
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.deleteFromTableLang("RelTypRoleLifeInterval", id);
    }
}
