package tofi.mdl.model.dao.reltyp;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;

public class RelTypMemberMdbUtils {
    Mdb mdb;

    public RelTypMemberMdbUtils(Mdb mdb) throws Exception {
        this.mdb = mdb;
        //
/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }
    //

    public Store loadRelTypMember(long reltyp) throws Exception {
        Store st = mdb.createStore("RelTypMember.full");
        mdb.loadQuery(st, """
                    select m.*,
                        case when m.membertype=1 then tv.name else rv.name end as memberName
                    from RelTypMember m
                        left join TypVer tv on tv.ownerver=m.typ and tv.lastVer=1
                        left join RelTypVer rv on rv.ownerver=m.reltypmemb and rv.lastVer=1
                    where m.reltyp=:rt
                    order by m.ord
                """, Map.of("rt", reltyp));
        return st;
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
        st = mdb.createStore("RelTypMember.full");
        mdb.loadQuery(st, """
                    select m.*,
                        case when m.membertype=3 then rv.name else tv.name end as memberName
                    from RelTypMember m
                        left join TypVer tv on tv.ownerver=m.typ and tv.lastVer=1
                        left join RelTypVer rv on rv.ownerver=m.reltypmemb and tv.lastVer=1
                    where m.id=:id
                """, Map.of("id", id));
        return st;
    }

    public Store updateRelTypMember(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long id = UtCnv.toLong(rec.get("id"));
        Store st = mdb.createStore("RelTypMember");
        StoreRecord r = st.add(rec);
        //
        mdb.updateRec("RelTypMember", r);

        //
        st = mdb.createStore("RelTypMember.full");
        mdb.loadQuery(st, """
                    select m.*,
                        case when m.membertype=3 then rv.name else tv.name end as memberName
                    from RelTypMember m
                        left join TypVer tv on tv.ownerver=m.typ and tv.lastVer=1
                        left join RelTypVer rv on rv.ownerver=m.reltypmemb and tv.lastVer=1
                    where m.id=:id
                """, Map.of("id", id));
        return st;
    }

    public void deleteRelTypMember(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        //
        validate(UtCnv.toLong(rec.get("relTyp")), "del");
        //
        mdb.deleteRec("RelTypMember", id);
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


    //todo
    public Store loadRelTypForSelect() throws Exception {
        return mdb.loadQuery("""
                    select t.id, v.name from RelTyp t, RelTypVer v where t.id=v.ownerVer and v.lastVer=1
                """);
    }

    public Store loadTypForSelect(Map<String, Object> params) throws Exception {
        return mdb.loadQuery("""
                    select t.id, v.name from Typ t, TypVer v where t.id=v.ownerVer and v.lastVer=1
                """);
    }

}
