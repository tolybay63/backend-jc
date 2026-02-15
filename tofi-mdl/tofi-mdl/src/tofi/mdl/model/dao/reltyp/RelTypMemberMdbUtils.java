package tofi.mdl.model.dao.reltyp;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.commons.variant.IVariantMap;
import jandcode.commons.variant.VariantMap;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.HashMap;
import java.util.Map;

public class RelTypMemberMdbUtils {
    Mdb mdb;

    public RelTypMemberMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
        //
    }
    //

    public Store loadRelTypMember(long reltyp, String lang) throws Exception {
        Store st = mdb.createStore("RelTypMember.lang");
        mdb.loadQuery(st, """
                    select m.*,
                        case when m.memberType=1 then l1.name else l2.name end as name,
                        case when m.memberType=1 then l1.fullName else l2.fullName end as fullName,
                        case when m.memberType=1 then l1.cmt else l2.cmt end as cmt
                    from RelTypMember m
                        left join TypVer tv on tv.ownerVer=m.typ and tv.lastVer=1
                        left join TableLang l1 on l1.nameTable='TypVer' and l1.idTable=tv.id and l1.lang=:lang
                        left join RelTypVer rv on rv.ownerVer=m.reltypMemb and rv.lastVer=1
                        left join TableLang l2 on l2.nameTable='RelTypVer' and l2.idTable=rv.id and l2.lang=:lang
                    where m.reltyp=:rt
                    order by m.ord
                """, Map.of("rt", reltyp, "lang", lang));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelTypMember", lang);
    }

    public Store loadRelTypMemberRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("RelTypMember.lang");
        mdb.loadQuery(st, """
                    select m.*,
                        case when m.memberType=1 then l1.name else l2.name end as name,
                        case when m.memberType=1 then l1.fullName else l2.fullName end as fullName,
                        case when m.memberType=1 then l1.cmt else l2.cmt end as cmt
                    from RelTypMember m
                        left join TypVer tv on tv.ownerVer=m.typ and tv.lastVer=1
                        left join TableLang l1 on l1.nameTable='TypVer' and l1.idTable=tv.id and l1.lang=:lang
                        left join RelTypVer rv on rv.ownerVer=m.reltypMemb and rv.lastVer=1
                        left join TableLang l2 on l2.nameTable='RelTypVer' and l2.idTable=rv.id and l2.lang=:lang
                    where m.id=:id
                """, Map.of("id", id, "lang", lang));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelTypMember", lang);

    }

    public Store insertRelTypMember(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        Store st = mdb.createStore("RelTypMember");
        StoreRecord r = st.add(rec);
        if (r.getLong("role") == 0) r.set("role", null);
        //
        long id = mdb.getNextId("RelTypMember");
        r.set("id", id);
        r.set("ord", id);
        mdb.insertRec("RelTypMember", r, false);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        Map<String, Object> map = new HashMap<>();
        map.put("table", "RelTypMember");
        map.put("id", id);
        map.put("lang", UtCnv.toString(rec.get("lang")));
        map.put("name", UtCnv.toString(rec.get("name")));
        map.put("fullName", UtCnv.toString(rec.get("fullName")));
        map.put("cmt", UtCnv.toString(rec.get("cmt")));
        ut.insertToTableLang(map);
        //
        return loadRelTypMemberRec(id, UtCnv.toString(rec.get("lang")));
    }

    public Store updateRelTypMember(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("RelTypMember");
        StoreRecord r = st.add(rec);
        //
        mdb.updateRec("RelTypMember", r);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        IVariantMap map = new VariantMap(rec);
        st = mdb.loadQuery("""
            select * from TableLang where nameTable=:table and idTable=:id and lang=:lang
        """, Map.of("table", "RelTypMember", "id", id, "lang", map.getString("lang")));
        map.put("id", st.get(0).getLong("id"));
        map.put("nameTable", "RelTypMember");
        map.put("idTable", id);
        ut.updateTableLang(map);
        //
        return loadRelTypMemberRec(id, map.getString("lang"));
    }

    public void deleteRelTypMember(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        //
        validate(UtCnv.toLong(rec.get("relTyp")), "del");
        //
        mdb.deleteRec("RelTypMember", id);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        ut.deleteFromTableLang("RelTypMember", id);
    }

    protected void validate(long reltyp, String flag) throws Exception {
        Store st = mdb.loadQuery("""
                    select rc.id
                    from RelCls rc
                    inner join RelClsMember m on rc.id=m.relcls
                    where rc.reltyp=:rt
                """, Map.of("rt", reltyp));
        String txt = "менять порядок участников";
        if (flag.equals("ins"))
            txt = "добавить участника";
        else if (flag.equals("del"))
            txt = "удалить участника";


        if (st.size() > 0) {
            throw new XError("Нельзя " + txt + ", т.к. существует класс отношений");
        }
    }

    public void changeOrdMember(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        boolean up = UtCnv.toBoolean(params.get("up"));
        long reltyp = UtCnv.toLong(rec.get("relTyp"));
        long id1 = UtCnv.toLong(rec.get("id"));
        long ord1 = UtCnv.toLong(rec.get("ord"));
        long id2 = 0;
        long ord2 = 0;
        //
        validate(reltyp, "ord");
        //
        Store st = mdb.loadQuery("""
                    select * from RelTypMember where reltyp=:rt order by ord
                """, Map.of("rt", reltyp));
        int k = 0;  //искомая позиция
        for (int i = 0; i < st.size(); i++) {
            if (st.get(i).getLong("id") == id1) {
                k = i;
                break;
            }
        }
        if (up) {
            id2 = st.get(k - 1).getLong("id");
            ord2 = st.get(k - 1).getLong("ord");
        } else {
            id2 = st.get(k + 1).getLong("id");
            ord2 = st.get(k + 1).getLong("ord");
        }
        //
        mdb.execQuery("""
                    update RelTypMember set ord=:ord2 where id=:id1;
                    update RelTypMember set ord=:ord1 where id=:id2;
                """, Map.of("id1", id1, "id2", id2, "ord1", ord1, "ord2", ord2));
    }


    public Store loadRelTypForSelect(String lang) throws Exception {
        Store st = mdb.createStore("RelTyp.lang");
        mdb.loadQuery(st,"""
          select t.*, v.id as verId, v.dbeg, v.dend
          from RelTyp t
            left join RelTypVer v on t.id=v.ownerVer and v.lastVer=1
          where 0=0
        """);
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelTyp", lang, true);
    }

    public Store loadTypForSelect(String lang) throws Exception {
        Store st = mdb.createStore("Typ.lang");
        mdb.loadQuery(st,"""
          select t.id, l.name
          from Typ t
            left join TypVer tv on t.id=tv.ownerVer and tv.lastVer=1
            left join TableLang l on l.nameTable='TypVer' and l.idTable=tv.id and l.lang=:lang
          where 0=0
        """, Map.of("lang", lang));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Typ", lang, true);
    }

    public Store loadRoleForSelect(Map<String, Object> params) throws Exception {
        long reltyp = UtCnv.toLong(params.get("relTyp"));
        String lang = UtCnv.toString(params.get("lang"));

        Store st = mdb.createStore("Role.lang");
        mdb.loadQuery(st,"""
          select t.id, l2.name, l2.cmt
          from Role t
            left join TableLang l2 on l2.nameTable='Role' and l2.idTable=t.id and l2.lang=:lang
          where t.id not in (select role from RelTypMember where reltyp=:rt)
        """, Map.of("lang", lang, "rt", reltyp));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Role", lang);


    }

}
