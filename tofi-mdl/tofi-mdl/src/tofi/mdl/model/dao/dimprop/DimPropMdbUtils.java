package tofi.mdl.model.dao.dimprop;

import jandcode.commons.UtCnv;
import jandcode.commons.error.XError;
import jandcode.core.auth.AuthService;
import jandcode.core.auth.AuthUser;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreIndex;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_AccessLevel_consts;
import tofi.mdl.consts.FD_DimPropType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DimPropMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public DimPropMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
        //
/*
        if (!mdb.getApp().getEnv().isTest())
            if (!UtCnv.toBoolean(mdb.createDao(AuthDao.class).isLogined().get("success")))
                throw new XError("notLogined");
*/
    }

    /**
     * @param dimPropGr
     * @return
     * @throws Exception
     */
    public Store loadDimProp(long dimPropGr) throws Exception {
        Store st = mdb.createStore("DimProp.full");
        return mdb.loadQuery(st, """
                    select dp.*, dmp.dimMultiPropType
                    from DimProp dp
                        left join DimMultiProp dmp on dp.dimmultiprop=dmp.id
                    where dimPropGr=:g
                """, Map.of("g", dimPropGr));
    }

    Map<String, Long> getDimMultiPropType(long dimProp) throws Exception {
        StoreRecord rec = mdb.loadQuery("""
                select dmp.id as dmp, dmp.dimmultiproptype as dmpt
                from DimProp dp
                	left join DimMultiProp dmp on dp.dimmultiprop=dmp.id
                	where dp.id=:dp
        """, Map.of("dp", dimProp)).get(0);

        return Map.of("dmp", rec.getLong("dmp"), "dmpt", rec.getLong("dmpt"));
    }

    /**
     * @param dimPropGroup
     * @return
     * @throws Exception
     */
    public StoreRecord newRec(long dimPropGroup) throws Exception {
        Store st = mdb.createStore("DimProp");
        StoreRecord r = st.add();
        r.set("dimPropGr", dimPropGroup);
        r.set("dimPropType", FD_DimPropType_consts.prop);
        r.set("accessLevel", FD_AccessLevel_consts.common);
        return r;
    }

    public StoreRecord loadRec(long id) throws Exception {
        Store st = mdb.createStore("DimProp");
        StoreRecord rec = st.add();
        return mdb.loadQueryRecord(rec, """
                    select * from DimProp where id=:id
                """, Map.of("id", id));
    }

    /**
     * @param params
     * @return
     * @throws Exception
     */
    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntity(rec);
        //
        Store st = mdb.createStore("DimProp");
        mdb.loadQuery(st, "select * from DimProp where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    /**
     * @param params
     * @return
     * @throws Exception
     */
    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntity(rec);
        //
        // Загрузка записи
        Store st = mdb.createStore("DimProp");
        mdb.loadQuery(st, "select * from DimProp where id=:id", Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    /**
     * @param map
     * @throws Exception
     */
    public void delete(Map<String, Object> map) throws Exception {
        deleteEntity(UtCnv.toMap(map.get("rec")));
    }

    //?????
    public Store loadDimPropItemPropForUpd(long dimprop) throws Exception {
        //old values
        //Store stOld = loadDimPropItemProp(Map.of("dimprop", dimprop));
        //StoreIndex indStOld = stOld.getIndex("id");

        Store st = mdb.createStore("DimPropItem.prop.checked");
        String sql = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr,
                    null as prop, null as propType, cod, name, fullName, false as checked
                    from PropGr where 0=0
                    union all
                    select 'p_'||id as id,
                    case when parent is null then 'g_'||propGr else 'p_'||parent end as parent, propgr,
                    id as prop, propType, cod, name, fullName, false as checked
                    from Prop
                    where 0=0
                """;
        mdb.loadQuery(st, sql);
        List<String> lst = new ArrayList<>();


        return st;
    }
    //????

    public Store loadDimPropItemProp(long dimprop, long dimpropType) throws Exception {
        Store st = mdb.createStore("DimPropItem.prop");
        String sql = """
            select d.*, p.proptype from DimPropItem d, Prop p where d.dimprop=:dp and d.prop=p.id
        """;

        if (dimpropType != FD_DimPropType_consts.prop) {
            st = mdb.createStore("DimPropItem");
            sql = """
                select * from DimPropItem where dimprop=:dp
            """;
        }

        mdb.loadQuery(st, sql, Map.of("dp", dimprop));
        return st;
    }

    //todo 09.02.25
    public Store loadAllDimMultiPropForSelect() throws Exception {
        Store st = mdb.createStore("DimMultuProp.select");
        return mdb.loadQuery(st, """
            select -id as id, parent, name, 0 as dimMultiProp, 0 as dimMultiPropType
            from DimMultiPropGr where 0=0
            union all
            select id, -dimMultiPropGr as parent, name, id as dimMultiProp, dimMultiPropType
            from DimMultiProp where 0=0
        """);
    }

    public Store loadAllDimMultiPropItemForSelect() throws Exception {
        Store st = mdb.createStore("DimPropItem.dimMultiPropItem");
        return mdb.loadQuery(st, """
            select -id as id, parent, name, 0 as dimMultiProp, 0 as dimMultiPropType
            from DimMultiPropGr where 0=0
            union all
            select id, -dimMultiPropGr as parent, name, id as dimMultiProp, dimMultiPropType
            from DimMultiProp where 0=0
        """);
    }

    public Store loadAllMultiPropForSelect() throws Exception {
        return mdb.loadQuery("""
            select -id as id, parent, name, fullName, 0 as multiProp
            from MultiPropGr
            union all
            select id, -multiPropGr as parent, name, fullName, id as multiProp
            from MultiProp
        """);
    }

    public Store insertDPI(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("DimPropItem");
        StoreRecord r = st.add(rec);
        StoreRecord recDP = loadRec(r.getLong("dimProp"));
        long dimPropTyp = recDP.getLong("dimPropType");
        long id = mdb.getNextId("DimPropItem");
        r.set("id", id);
        r.set("ord", id);
        mdb.insertRec("DimPropItem", r, false);
        if (dimPropTyp == FD_DimPropType_consts.factor) {
            String fvs = UtCnv.toString(rec.get("fvs"));
            List<String> lstFvs = List.of(fvs.split(","));
            Store stFV = mdb.createStore("DimPropItemFV");
            for (String sfv : lstFvs) {
                long fv = UtCnv.toLong(sfv);
                StoreRecord rr = stFV.add();
                rr.set("dimPropItem", id);
                rr.set("factorVal", fv);
                try {
                    mdb.insertRec("DimPropItemFV", rr, true);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return st;
    }

    public Store updateDPI(Map<String, Object> rec) throws Exception {
        Store stRes = mdb.createStore("DimPropItem");
        StoreRecord r = stRes.add(rec);
        StoreRecord recDP = loadRec(r.getLong("dimProp"));
        long dimPropTyp = recDP.getLong("dimPropType");
        mdb.updateRec("DimPropItem", r);

        if (dimPropTyp == FD_DimPropType_consts.factor) {
            String fvs = UtCnv.toString(rec.get("fvs"));
            if (fvs.isEmpty()) return stRes;                    //todo
            //new fvs
            List<Object> newFvs = List.of(fvs.split(","));
            //old fvs
            Store stOld = mdb.loadQuery("select id, factorVal from DimPropItemFV where dimPropItem=:dpi",
                    Map.of("dpi", r.getLong("id")));
            Set<Object> oldFvs = stOld.getUniqueValues("factorVal");
            //Deleting
            for (StoreRecord rr : stOld) {
                if (!newFvs.contains(rr.get("id"))) {
                    mdb.deleteRec("DimPropItemFV", rr.getLong("id"));
                }
            }
            // Saving
            Store st = mdb.createStore("DimPropItemFV");
            for (Object fv : newFvs) {
                if (!oldFvs.contains(fv)) {
                    StoreRecord rr = st.add();
                    rr.set("dimPropItem", r.getLong("id"));
                    rr.set("factorVal", UtCnv.toLong(fv));
                    mdb.insertRec("DimPropItemFV", rr, true);
                }
            }
        }
        return stRes;
    }

    public void deleteDPI(long id) throws Exception {
        try {
            mdb.execQuery("""
                        delete from DimPropItemFV where dimPropItem=:dpi
                    """, Map.of("dpi", id));
        } finally {
            mdb.deleteRec("DimPropItem", id);
        }

    }

    public Store loadForFvSelect(long dimPropItem) throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class);
        AuthUser au = authSvc.getCurrentUser();
        long al = au.getAttrs().getLong("accesslevel");

        Store st = mdb.createStore("FactorVal.select");
        mdb.loadQuery(st, """
                    select id, name, parent, false as checked from Factor where accessLevel<=:al order by ord
                """, Map.of("al", al));

        if (dimPropItem == 0)
            return st;

        StoreIndex stInd = st.getIndex("id");
        Store stFV = mdb.loadQuery("""
                    select factorVal from dimpropitemfv
                    where dimpropitem = :dpi
                """, Map.of("dpi", dimPropItem));
        Set<Object> setFvs = stFV.getUniqueValues("factorVal");

        for (Object fvo : setFvs) {
            long fv = UtCnv.toLong(fvo);
            StoreRecord r = stInd.get(fv);
            if (r != null) {
                StoreRecord rp = stInd.get(r.getLong("parent"));
                r.set("checked", true);
                rp.set("checked", true);
            }
        }
        return st;
    }

    public Store loadDimMultiPropItemForSelect(long dimMultiProp) throws Exception {
        return mdb.loadQuery("""
            select id, parent, name, fullname
            from DimMultiPropItem
            where dimmultiprop=:dmp and multientitytype=1
        """, Map.of("dmp", dimMultiProp));
    }


    //todo Delete!
    /*public Store loadStatusForSelect() throws Exception {
        return mdb.loadQuery("""
            select * from (
              select id, parent, name, fullname, -id as status, ord
              from factor
              where id in (
                select parent
                from factor
                where id in (select factorval from PropStatus where 0=0)
              )
              union all
              select id, parent, name, fullname, id as status, ord
              from factor
              where id in (select factorval from PropStatus where 0=0)
            ) t
            order by ord
        """);
    }*/

    //todo Delete!
    /*public Store loadProviderForSelect(long dimProp, String mode) throws Exception {
        String whe = "";
        if (mode.equals("ins"))
            whe = " and c.id not in (select coalesce(cls, 0) as cls from DimPropItem where dimProp=:dp)";

        String sql = """
            select c.id, name, fullname from Cls c, ClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and c.id in (select cls from PropProvider)
        """ + whe;

        return mdb.loadQuery(sql, Map.of("dp", dimProp));
    }*/



}
