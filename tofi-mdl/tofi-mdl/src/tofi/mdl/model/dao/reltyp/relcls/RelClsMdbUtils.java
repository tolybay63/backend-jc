package tofi.mdl.model.dao.reltyp.relcls;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.std.CfgService;
import jandcode.core.store.Store;
import jandcode.core.store.StoreIndex;
import jandcode.core.store.StoreRecord;
import tofi.api.dta.*;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;
import tofi.mdl.consts.FD_MemberType_consts;
import tofi.mdl.consts.FD_PropType_consts;
import tofi.mdl.model.dao.typ.ClsTreeMdbUtils;
import tofi.mdl.model.utils.CartesianProduct;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.*;

public class RelClsMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public RelClsMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
        //
    }

    ApinatorApi apiMonitoringData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("monitoringdata");
    }
    ApinatorApi apiUserData() {
        return  mdb.getApp().bean(ApinatorService.class).getApi("userdata");
    }
    ApinatorApi apiNSIData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("nsidata");
    }
    ApinatorApi apiObjectData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("objectdata");
    }
    ApinatorApi apiOrgStructureData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("orgstructuredata");
    }
    ApinatorApi apiPersonnalData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("personnaldata");
    }
    ApinatorApi apiPlanData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("plandata");
    }

    //---------------------------------------------------

    public Store load(long id, long relTyp, String lang) throws Exception {
        Store st = mdb.createStore("RelCls.lang");
        String whe = "c.relTyp=:relTyp";
        if (id>0)
            whe = "c.id=:id";

        mdb.loadQuery(st, """
            select *, v.id as verId
            from RelCls c
                left join RelClsVer v on c.id=v.ownerVer and v.lastVer=1
                left join TableLang l on l.nameTable='RelClsVer' and l.idTable=v.id and l.lang=:lang
                where
        """+whe, Map.of("relTyp", relTyp, "lang", lang));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelCls", lang, true);
    }


    public Store insert(Map<String, Object> rec) throws Exception {
        //
        //long id = insertEntityWithVer(rec);
        long id = insertEntity(rec);
        //
        Store st = mdb.createStore("RelCls.full");
        mdb.loadQuery(st, """
                    select * from RelCls t, RelClsVer v where t.id=v.ownerVer and v.lastver=1 and t.id=:id
                """, Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    public Store update(Map<String, Object> rec) throws Exception {
        //
        //updateEntityWithVer(rec);
        updateEntity(rec);
        //
        Store st = mdb.createStore("RelCls.full");
        mdb.loadQuery(st, """
                    select * from RelCls t, RelClsVer v where t.id=v.ownerVer and v.lastver=1 and t.id=:id
                """, Map.of("id", UtCnv.toLong(rec.get("id"))));
        mdb.resolveDicts(st);
        return st;
    }

    protected void checkExistOwners(long relcls, String modelMeta) {
        List<String> lstApp = new ArrayList<>();
        if (modelMeta.equalsIgnoreCase("fish")) {
            //1
            boolean b = apiUserData().get(ApiUserData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("userdata");
            //2
            b = apiNSIData().get(ApiNSIData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("nsidata");
            //3
            b = apiMonitoringData().get(ApiMonitoringData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("monitoringdata");
        }

        if (modelMeta.equalsIgnoreCase("dtj")) {
            boolean b = apiUserData().get(ApiUserData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("userdata");
            b = apiNSIData().get(ApiNSIData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("nsidata");
            b = apiObjectData().get(ApiObjectData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("objectdata");
            b = apiOrgStructureData().get(ApiOrgStructureData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("orgstructuredata");
            b = apiPersonnalData().get(ApiPersonnalData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("personnaldata");
            b = apiPlanData().get(ApiPlanData.class).checkExistOwners(relcls, false);
            if (b) lstApp.add("plandata");
        }

        if (!lstApp.isEmpty()) {
            throw new XError("ExistRelObjInApp@"+UtString.join(lstApp, ", "));
        }

    }

    public void delete(Map<String, Object> rec) throws Exception {

        //---< check data in other DB
        CfgService cfgSvc = mdb.getApp().bean(CfgService.class);
        String modelMeta = cfgSvc.getConf().getString("dbsource/default/id");
        if (modelMeta.isEmpty())
            throw new XError("Не найден id мета модели");

        checkExistOwners(UtCnv.toLong(rec.get("id")), modelMeta);
        //--->

        deleteEntity(rec);
    }

    public Store loadRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("RelCls.lang");
        mdb.loadQuery(st, """
            select c.id, c.cod, c.reltyp, v.id as verId
            from RelCls c, RelClsVer v
            where c.id=v.ownerVer and v.lastVer=1 and c.id=:id
        """, Map.of("id", id));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelCls", lang, true);
    }

    //todo ClsVer => RelClsVer
    public Store loadVer(long relcls, String lang) throws Exception {
        Store st = mdb.createStore("RelClsVer.lang");
        mdb.loadQuery(st, """
            select * from
            RelCls c, RelClsVer v, TableLang l
            where c.id=:id and c.id=v.ownerVer and v.lastVer=1
                and l.nameTable='RelClsVer' and l.idTable=v.id and l.lang=:lang
            order by dend desc
        """, Map.of("id", relcls, "lang", lang));
        return st;
    }

    public Store insertVer(Map<String, Object> rec) throws Exception {
        //
        long id = insertEntityVer(rec);
        //
        Store st = mdb.createStore("RelClsVer");
        mdb.loadQuery(st, "select * from RelClsVer where id=:id", Map.of("id", id));

        return st;
    }

    public Store updateVer(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntityVer(rec);
        // Загрузка записи
        Store st = mdb.createStore("RelClsVer");
        mdb.loadQuery(st, "select * from RelClsVer where id=:id", Map.of("id", id));
        return st;
    }

    public void deleteVer(Map<String, Object> rec) throws Exception {
        deleteEntityVer(rec);
    }

    // RelClsMember
    public Store loadRelClsMember(long relcls, String lang) throws Exception {
        Store st = mdb.createStore("RelClsMember.lang");
        mdb.loadQuery(st, """
                    select m.*,
                        case when l1.id is not null then l1.name else l2.name end as name,
                        case when l1.id is not null then l1.fullname else l2.fullname end as fullName
                    from RelClsMember m
                        left join ClsVer cv on cv.ownerVer=m.cls and cv.lastVer=1
                        left join TableLang l1 on l1.nameTable='ClsVer' and l1.idTable=cv.id and l1.lang=:lang
                        left join RelClsVer rv on rv.ownerVer=m.relclsMemb and rv.lastVer=1
                        left join TableLang l2 on l2.nameTable='RelClsVer' and l2.idTable=rv.id and l2.lang=:lang
                    where m.relcls=:rc
                    order by m.id
                """, Map.of("rc", relcls, "lang", lang));

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelClsMember", lang);
    }


    public Store loadRelClsMemberRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("RelClsMember.lang");
        mdb.loadQuery(st, """
                    select m.*,
                        case when l1.id is not null then l1.name else l2.name end as name,
                        case when l1.id is not null then l1.fullname else l2.fullname end as fullName
                    from RelClsMember m
                        left join ClsVer cv on cv.ownerVer=m.cls and cv.lastVer=1
                        left join TableLang l1 on l1.nameTable='ClsVer' and l1.idTable=cv.id and l1.lang=:lang
                        left join RelClsVer rv on rv.ownerVer=m.relclsMemb and rv.lastVer=1
                        left join TableLang l2 on l2.nameTable='RelClsVer' and l2.idTable=rv.id and l2.lang=:lang
                    where m.id=:id
                    order by m.id
                """, Map.of("id", id, "lang", lang));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelClsMember", lang);
    }



    public Store loadAllMembers(Map<String, Object> params) throws Exception {
        UtEntityTranslate ut = new UtEntityTranslate(mdb);

        long reltyp = UtCnv.toLong(params.get("relTyp"));
        String lang = UtCnv.toString(params.get("lang"));

        Store stRes = mdb.createStore("RelClsMember.all");
        ClsTreeMdbUtils utCls = new ClsTreeMdbUtils(mdb);

        Store stRTM = mdb.loadQuery("""
                    select id, reltyp, card, memberType,
                        case when memberType=:mt then typ else reltypMemb end as ent, ord
                    from RelTypMember
                    where reltyp=:rt
                    order by ord
                """, Map.of("mt", FD_MemberType_consts.typ, "rt", reltyp));

        int ind = 0;
        for (StoreRecord r : stRTM) {
            ind++;
            Store stCur = mdb.createStore("RelClsMember.all");
            if (r.getLong("memberType") == FD_MemberType_consts.typ) {
                Store stCls = utCls.loadClsTree(Map.of("typ", r.getLong("ent"), "lang", lang));
                stCur.add(stCls);
                for (StoreRecord rec : stCur) {
                    rec.set("memberType", FD_MemberType_consts.cls);
                    rec.set("card", r.getInt("card"));
                    rec.set("checked", false);
                    rec.set("id", rec.getString("id") + "_" + ind);
                    if (rec.isValueNull("parent"))
                        rec.set("ord", ind);
                    else
                        rec.set("parent", rec.getString("parent") + "_" + ind);
                }

            } else {
                Store stRelTyp = mdb.createStore("RelClsMember.all");
                mdb.loadQuery(stRelTyp, """
                    select 'r'||'_'|| c.id as id, c.cod,-- v.name, v.fullName,
                    -1 as isOwn, false as checked, c.id as ent
                    from RelTyp c
                        left join RelTypVer v on c.id=:id and c.id=v.ownerVer and v.lastVer=1
                        --left join TableLang l on l.nameTable='RelTypVer' and l.idTable=v.id and l.lang=:lang
                    where 0=0
                """, Map.of("id", r.getLong("ent"), "lang", lang));

                stRelTyp.get(0).set("id", stRelTyp.get(0).getString("id")+'_'+ind);
                stRelTyp.get(0).set("ord", ind);
                //ut.getTranslatedStore(stRelTyp,"RelTyp", lang, true);
                stCur.add(ut.getTranslatedStore(stRelTyp,"RelTyp", lang, true).get(0));
                //
                Store stRelCls = mdb.createStore("RelClsMember.all");
                mdb.loadQuery(stRelCls, """
                    select *, c.id as ent
                    from RelCls c
                        left join RelClsVer v on c.reltyp=:id and c.id=v.ownerVer and v.lastVer=1
                        --left join TableLang l on l.nameTable='RelClsVer' and l.idTable=v.id and l.lang=:lang
                    where 0=0
                    order by c.ord
                """, Map.of("id", r.getLong("ent"), "lang", lang));

                stCur.add(ut.getTranslatedStore(stRelCls,"RelCls", lang, true));
                for (StoreRecord rec : stCur) {
                    if (!rec.getString("id").startsWith("r_")) {
                        rec.set("memberType", FD_MemberType_consts.relcls);
                        rec.set("card", r.getInt("card"));
                        rec.set("checked", false);
                        rec.set("isOwn", -2);
                        rec.set("parent", stRelTyp.get(0).getString("id"));
                        rec.set("id", rec.getString("id") + "_" + ind);
                    }
                }
            }
            stRes.add(stCur);
        }

        //mdb.outTable(stRes);

        return stRes;
    }

    private List<List<Object>> combAll(long relTyp) throws Exception {

        Store stRelCls = mdb.loadQuery
                ("select id from RelCls where reltyp=:rt order by ord", Map.of("rt", relTyp));

        List<List<Object>> lists = new ArrayList<>();

        for (StoreRecord r : stRelCls) {
            Store stMembCls = mdb.loadQuery
                    ("select * from relclsmember where relcls=:c", Map.of("c", r.getLong("id")));

            List<Object> lst = new ArrayList<>();
            for (StoreRecord rr : stMembCls ) {
                if (rr.getLong("membertype")==FD_MemberType_consts.cls) {
                    lst.add(rr.getLong("cls"));
                } else {
                    lst.add(rr.getLong("relclsmemb"));
                }
            }
            lists.add(lst);
        }

        return lists;

    }

    public void createGroupRelCls(long relTyp, List<List<Map<String, Object>>> lists, long db, String lang) throws Exception {

        List<List<Object>> lstlstAll = combAll(relTyp);

        List<List<Map<String, Object>>> listsNew = new ArrayList<>();

        Set<Object> setCls = new HashSet<>();
        Set<Object> setRel = new HashSet<>();

        for (List<Map<String, Object>> lst : lists) {
            List<Map<String, Object>> lstNew = new ArrayList<>();

            lst.forEach((Map<String, Object> l) -> {
                System.out.println(l);
                int memType = UtCnv.toInt(l.get("memType"));
                long cls = UtCnv.toLong(l.get("ent"));
                if (!setCls.contains(cls)) {

                    lstNew.add(l);

                    if (memType == FD_MemberType_consts.cls)
                        setCls.add(UtCnv.toLong(cls));
                    else if (memType == FD_MemberType_consts.relcls)
                        setRel.add(UtCnv.toLong(cls));
                    else
                        throw new XError("Unknown memberTyp: " + memType);
                }
            });
            listsNew.add(lstNew);
        }

        String wheIds = UtString.join(setCls, ",");
        wheIds = wheIds.isEmpty() ? "(0)" : "(" + wheIds + ")";
        String wheIdsRel = UtString.join(setRel, ",");
        wheIdsRel = wheIdsRel.isEmpty() ? "(0)" : "(" + wheIdsRel + ")";
        //

        Store stCls = mdb.createStore("Cls.lang");
        mdb.loadQuery(stCls, """
                    select c.id, v.id as verId, l.name, l.fullName, dataBase
                    from Cls c
                    left join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                    left join TableLang l on l.nameTable='ClsVer' and l.idTable=v.id and l.lang=:lang
                    where c.id in
                """ + wheIds, Map.of("lang", lang));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        stCls = ut.getTranslatedStore(stCls, "Cls", lang, true);
        StoreIndex indCls = stCls.getIndex("id");
        //
        Store stRelCls = mdb.createStore("RelCls.lang");
        mdb.loadQuery(stRelCls, """
            select c.id, v.id as verId, l.name, l.fullName, dataBase
            from RelCls c
            left join RelClsVer v on c.id=v.ownerVer and v.lastVer=1
            left join TableLang l on l.nameTable='RelClsVer' and l.idTable=v.id and l.lang=:lang
            where c.id in
        """ + wheIdsRel, Map.of("lang", lang));

        stRelCls = ut.getTranslatedStore(stRelCls, "RelCls", lang, true);
        StoreIndex indRelCls = stRelCls.getIndex("id");
        //
        List<List<Map<String, Object>>> lstlstUch = CartesianProduct.result(listsNew);

        Map<String, Object> mapRelClsMem = new HashMap<>();
        lstlstUch.forEach((List<Map<String, Object>> lstUch) -> {
            List<Object> sNm = new ArrayList<>();
            List<Object> sFn = new ArrayList<>();
            Store stMemcls = mdb.createStore("RelClsMember.lang");
            lstUch.forEach((Map<String, Object> u) -> {
                int memType = UtCnv.toInt(u.get("memType"));
                int card = UtCnv.toInt(u.get("card"));
                long cls = UtCnv.toLong(u.get("ent"));
                StoreRecord r;
                if (memType == FD_MemberType_consts.cls) {
                    r = indCls.get(cls);
                    mapRelClsMem.put("cls", cls);
                    mapRelClsMem.put("relClsMemb", null);
                } else if (memType == FD_MemberType_consts.relcls) {
                    r = indRelCls.get(cls);
                    mapRelClsMem.put("cls", null);
                    mapRelClsMem.put("relClsMemb", cls);
                } else {
                    throw new XError("Unknown memberTyp: " + memType);
                }
                if (r != null) {
                    sNm.add(r.getString("name"));
                    sFn.add(r.getString("fullName"));
                    mapRelClsMem.put("name", r.getString("name"));
                    mapRelClsMem.put("fullName", r.getString("fullName"));
                }
                mapRelClsMem.put("memberType", memType);
                mapRelClsMem.put("card", card);
                stMemcls.add(mapRelClsMem);
            });
            String nm = UtString.join(sNm, " ⇔ ");
            String fn = UtString.join(sFn, " ⇔ ");
            long idRelCls = 0;
            Map<String, Object> map = new HashMap<>();
            map.put("relTyp", relTyp);
            map.put("name", nm);
            map.put("fullName", fn);
            try {
                StoreRecord recRelTyp = mdb.loadQueryRecord("""
                            select accessLevel, isOpenness from RelTyp where id=:id
                        """, Map.of("id", relTyp));
                map.put("accessLevel", recRelTyp.getLong("accessLevel"));
                map.put("isOpenness", recRelTyp.getLong("isOpenness"));
                //
                map.put("dataBase", db);
            } catch (Exception e) {
                e.printStackTrace();
            }

            List<Object> setUch = new ArrayList<>();
            for (StoreRecord r : stMemcls) {
                if (r.getLong("cls") > 0) {
                    setUch.add(r.getLong("cls"));
                } else {
                    setUch.add(r.getLong("relClsMemb"));
                }
            }

            if (!lstlstAll.contains(setUch)) {
                try {
                    RelClsMdbUtils utRC = new RelClsMdbUtils(mdb, "RelCls");
                    idRelCls = utRC.insertEntity(map);

                    // add to PropVal
                    Store rProp = mdb.loadQuery("select id, allItem from Prop where reltyp=:rt and propType=:pt",
                            Map.of("rt", relTyp, "pt", FD_PropType_consts.reltyp));
                    if (rProp.size() > 0) {
                        if (rProp.get(0).getBoolean("allItem")) {
                            long prop = rProp.get(0).getLong("id");
                            mdb.insertRec("PropVal", Map.of("prop", prop, "relCls", idRelCls), true);
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
                UtEntityTranslate utET = new UtEntityTranslate(mdb);
                for (StoreRecord r : stMemcls) {
                    r.set("relCls", idRelCls);
                    try {
                        StoreRecord rec = mdb.createStoreRecord("RelClsMember", r);
                        long idRCM = mdb.insertRec("RelClsMember", rec, true);
                        utET.insertToTableLang(Map.of("table", "RelClsMember", "id", idRCM,
                                "lang", lang, "name", r.getString("name"),
                                "fullName", r.getString("name"), "cmt", r.getString("cmt")));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    public String deleteGroupRelCls(long relTyp, List<Map<String, Object>> list) throws Exception {

        //1
        Store stRCG = mdb.loadQuery("""
            select * from RelTypCharGr where relcls in (select id from RelCls where reltyp=:rt)
        """, Map.of("rt", relTyp));
        StoreIndex indStRCG = stRCG.getIndex("relcls");
        //2
        Store stFT = mdb.loadQuery("""
            select * from FlatTable where relcls in (select id from RelCls where reltyp=:rt)
        """, Map.of("rt", relTyp));
        StoreIndex indStFT = stFT.getIndex("relcls");

        //---< check data in other DB
        CfgService cfgSvc = mdb.getApp().bean(CfgService.class);
        String modelMeta = cfgSvc.getConf().getString("dbsource/default/id");
        if (modelMeta.isEmpty())
            throw new XError("Не найден id мета модели");
        //-->

        StringBuilder sb = new StringBuilder();
        for (Map<String, Object> m : list) {
            StoreRecord rec1 = indStRCG.get(m.get("id"));
            StoreRecord rec2 = indStFT.get(m.get("id"));

            if (rec1 != null || rec2 != null) {
                if (rec1 != null)
                    sb.append("\n").append(m.get("cod")).append(": ").append(rec1.getString("cod"));
                if (rec2 != null)
                    sb.append("\n").append(m.get("cod")).append(": ").append(rec2.getString("cod"));
            } else {
                long relcls = UtCnv.toLong(m.get("id"));
                checkExistOwners(relcls, modelMeta);
                deleteEntity(m);
            }
        }

        return sb.toString();
    }

    public Store updateRelClsMember(Map<String, Object> rec) throws Exception {
        StoreRecord r = mdb.createStoreRecord("RelClsMember", rec);
        if (r.getLong("id") == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        mdb.updateRec("RelClsMember", r);
        // Загрузка записи
        Store st = mdb.createStore("RelClsMember");
        mdb.loadQuery(st, "select * from RelClsMember where id=:id", Map.of("id", r.getLong("id")));
        return st;
    }


}
