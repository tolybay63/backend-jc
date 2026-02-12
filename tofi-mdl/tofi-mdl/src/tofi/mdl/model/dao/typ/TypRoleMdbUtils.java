package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class TypRoleMdbUtils {
    Mdb mdb;

    public TypRoleMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
    }

    public Store loadTypRole(long id, long typ, String lang) throws Exception {

        String whe = "t.typ=:typ";
        if (id > 0) {
            whe = "t.id=:id";
        }

        UtEntityTranslate ut = new UtEntityTranslate(mdb);

        //Перевод TypRole
/*
        Store stTypRole = mdb.createStore("TypRole.lang");
        mdb.loadQuery(stTypRole, """
          select t.*, l2.cmt
          from TypRole t
            left join TableLang l2 on l2.nameTable='TypRole' and l2.idTable=t.id and l2.lang=:lang
          where
        """+whe, Map.of("id", id, "typ", typ, "lang", lang));
        ut.getTranslatedStore(stTypRole,"TypRole", lang);
*/
        //

        //Загрузка TypRole: name,fullName from Role, cmt from TypeRole
        Store stLoad = mdb.createStore("TypRole.full");
        mdb.loadQuery(stLoad, """
          select t.*, l.name, l.fullName, l2.cmt
          from TypRole t
            left join Role r on t.role = r.id
            left join TableLang l on l.nameTable='Role' and l.idTable=r.id and l.lang=:lang
            left join TableLang l2 on l2.nameTable='TypRole' and l2.idTable=t.id and l2.lang=:lang
          where
        """+whe, Map.of("id", id, "typ", typ, "lang", lang));

        return stLoad;
    }


    public Store selectTypRole(long typ, String lang) throws Exception {
        Store st = mdb.createStore("Role.lang");
        mdb.loadQuery(st,"""
          select t.id, l2.name, l2.cmt
          from Role t
            left join TableLang l2 on l2.nameTable='Role' and l2.idTable=t.id and l2.lang=:lang
          where t.id not in (select role from TypRole where typ=:t)
        """, Map.of("lang", lang, "t", typ));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Role", lang);
    }

    public Store insertTypRole(Map<String, Object> params) throws Exception {
        Store st = mdb.createStore("TypRole");
        StoreRecord r = st.add(params);
        //
        long id = mdb.insertRec("TypRole", r, true);
        //
        //
        Map<String, Object> map = new HashMap<>();
        map.put("table", "TypRole");
        map.put("id", id);
        map.put("lang", UtCnv.toString(params.get("lang")));
        map.put("cmt", UtCnv.toString(params.get("cmt")));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.insertToTableLang(map);

        return loadTypRole(id, 0, UtCnv.toString(params.get("lang")));
    }

    public Store updateTypRole(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        Store st = mdb.createStore("TypRole");
        StoreRecord r = st.add(params);
        //
        mdb.updateRec("TypRole", r);
        //
        return loadTypRole(id, 0, UtCnv.toString(params.get("lang")));
    }

    public void deleteTypRole(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.execQuery("""
            delete from TableLang
            where nameTable='TypRoleLifeInterval' and
                idTable in (select id from TypRoleLifeInterval where typRole=:tr);
            delete from TypRoleLifeInterval where typRole=:tr
        """, Map.of("tr", id));
        mdb.deleteRec("TypRole", id);
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.deleteFromTableLang("TypRole", id);
    }

    /////
    public Store loadTypRoleLife(long id, long typRole, String lang) throws Exception {
        Store st = mdb.createStore("TypRoleLifeInterval.lang");
        String whe = "typRole=:tr";
        if (id>0)
            whe = "id=:id";
        mdb.loadQuery(st, "select * from TypRoleLifeInterval where "+whe, Map.of("id", id, "tr", typRole));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        //
        return ut.getTranslatedStore(st,"TypRoleLifeInterval", lang);
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
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        Map<String, Object> map = new HashMap<>();
        map.put("table", "TypRoleLifeInterval");
        map.put("id", id);
        map.put("lang", UtCnv.toString(rec.get("lang")));
        map.put("cmt", UtCnv.toString(rec.get("cmt")));
        ut.insertToTableLang(map);
        //
        return loadTypRoleLife(id, 0, UtCnv.toString(rec.get("lang")));
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
        return loadTypRoleLife(id, 0, UtCnv.toString(rec.get("lang")));
    }

    public void deleteTypRoleLife(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        mdb.deleteRec("TypRoleLifeInterval", id);
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.deleteFromTableLang("TypRoleLifeInterval", id);
    }
}
