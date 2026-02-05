package tofi.mdl.model.dao.typ;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.std.CfgService;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.api.dta.*;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;
import tofi.mdl.consts.FD_PropType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ClsMdbUtils extends EntityMdbUtils {
    ApinatorApi apiUserData() {
        return  mdb.getApp().bean(ApinatorService.class).getApi("userdata");
    }
    ApinatorApi apiNSIData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("nsidata");
    }
    ApinatorApi apiMonitoringData() {
        return mdb.getApp().bean(ApinatorService.class).getApi("monitoringdata");
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


    Mdb mdb;
    String tableName;

    public ClsMdbUtils(Mdb mdb, String tableName) {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public Store loadCls(long typ, String lang) throws Exception {
        Store st = mdb.createStore("Cls.lang");
        mdb.loadQuery(st, """
            select *, v.id as verId
            from Cls c, ClsVer v, TableLang l
            where c.id=v.ownerVer and v.lastVer=1 and c.typ=:typ
                and l.nameTable='ClsVer' and l.idTable=v.id
        """, Map.of("typ", typ));

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Cls", lang, true);
    }

    public Store loadRecCls(long id, String lang) throws Exception {
        Store st = mdb.createStore("Cls.lang");
        mdb.loadQuery(st, """
            select *, v.id as verId
            from Cls c, ClsVer v, TableLang l
            where c.id=v.ownerVer and v.lastVer=1 and c.id=:id
                and l.nameTable='ClsVer' and l.idTable=v.id
        """, Map.of("id", id, "lang", lang));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"Cls", lang, true);
    }

    public Store loadClsFVforUpd(long typ, long cls, String lang) throws Exception {
        Store st = mdb.createStore("TypClusterFactor.tree");
        TypDao typDao = mdb.createDao(TypDao.class);
        long typParent = typDao.loadRec(Map.of("id", typ, "lang", lang)).get(0).getLong("parent");

        mdb.loadQuery(st, """
                    select a.*, case when c.id > 0 then 1 else 0 end as checked
                    from (
                        select id, name, fullName, cod, cmt, isReq, isUniq, parent, isOwn, ord
                        from
                            (
                                select -tcf.factor as id, l.name, l.fullName, l.cmt, f.cod, tcf.isReq, tcf.isUniq, null::numeric as parent,
                                    case when tcf.typ=:typ then 1 else 0 end as isOwn, f.ord
                                from TypClusterFactor tcf, Factor f, TableLang l
                                where (tcf.typ=:typ or tcf.typ=:typParent) and tcf.factor = f.id
                                    and l.nameTable='Factor' and l.idTable=f.id and l.lang=:lang
                            ) t
                        union all
                        select id, name, fullName, cod, cmt, isReq, isUniq, parent, isOwn, ord
                        from
                        (
                           select fv.id, l.name, l.fullName, l.cmt, fv.cod, tcf.isReq, tcf.isUniq, -fv.parent as parent,
                               case when tcf.typ=:typ then 1 else 0 end as isOwn, fv.ord
                           from TypClusterFactor tcf, Factor f, Factor fv, TableLang l
                           where (tcf.typ=:typ or tcf.typ=:typParent) and tcf.factor = f.id and f.id=fv.parent
                            and l.nameTable='Factor' and l.idTable=fv.id and l.lang=:lang
                        ) t
                    ) a left join ClsFactorVal c on c.factorVal=a.id and c.cls=:cls
                order by isOwn desc, ord
                """, Map.of("typ", typ, "typParent", typParent, "cls", cls, "lang", lang));
        return st;
    }

    public Store loadClsFV(long typ, long cls, String lang) throws Exception {
        Store st = mdb.createStore("TypClusterFactor.full");
        mdb.loadQuery(st, """
                select * from (
                  select distinct id, name, fullName, cod, parent, isOwn, ord
                  from
                      (
                          select -f.id as id, l.name, l.fullName, f.cod, null::numeric as parent,
                              case when f.id in (select factor from TypClusterFactor where typ=:t) then 1 else 0 end as isOwn, f.ord
                          from Factor f, ClsFactorVal cf, factor fv, TableLang l
                          where cf.cls=:c and cf.factorVal = fv.id and fv.parent=f.id
                            and l.nameTable='Factor' and l.idTable=f.id and l.lang=:lang
                      ) t
                  union all
                  select id, name, fullName, cod, parent, isOwn, ord
                  from
                      (
                          select fv.id, l.name, l.fullName, fv.cod, -fv.parent as parent,
                          case when fv.parent in (select factor from TypClusterFactor where typ=:t) then 1 else 0 end as isOwn, fv.ord
                          from Factor fv, ClsFactorVal cf, TableLang l
                          where cf.cls=:c and fv.parent is not null and cf.factorVal = fv.id
                            and l.nameTable='Factor' and l.idTable=fv.id and l.lang=:lang
                      ) t
                ) a
                order by ord
                """, Map.of("t", typ, "c", cls, "lang", lang));

        return st;
    }


    public Store insertCls(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long typ = UtCnv.toLong(rec.get("typ"));
        List<String> lstFvs = UtCnv.toList(params.get("ids"));
        String lang = UtCnv.toString(rec.get("lang"));
        // Проверка уаказанных значении классов на повторность
        int cnt = lstFvs.size();
        String fvs = lstFvs.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "", ""));

        testForDubleCls(fvs, cnt, 0, typ, "ins", lang);

        //add Cls
        long cls = insertEntity(rec);
        //add to PropVal
        Store rTmp = mdb.loadQuery("select id, allItem from Prop where typ=:typ and proptype=:pt",
                Map.of("typ", typ, "pt", FD_PropType_consts.typ));
        if (rTmp.size() > 0) {
            if (rTmp.get(0).getBoolean("allItem")) {
                long prop = rTmp.get(0).getLong("id");
                mdb.insertRec("PropVal", Map.of("prop", prop, "cls", cls), true);
            }
        }


        //add ClsFactorVal
        for (String sfv : lstFvs) {
            long f = UtCnv.toLong(sfv);
            mdb.insertRec("ClsFactorVal", Map.of("cls", cls, "factorVal", f), true);
        }
        //
        return loadRecCls(cls, lang);
    }

    protected void testForDubleCls(String fvs, int cnt, long cls, long typ, String mode, String lang) throws Exception {
        if (fvs.isEmpty())
            throw new XError("notClustFactorVal");

        Store ds;
        if (mode.equals("ins")) {
            ds = mdb.loadQuery("select cls from ClsFactorVal cl, Cls c where factorVal in (" + fvs + ") and " +
                            "cl.cls=c.id  and c.typ=" + typ + " group by cls having count(*)=:cnt and count(*)=(select count(*) from ClsFactorVal where cls=cl.cls group by cls)",
                    Map.of("fvs", fvs, "cnt", cnt));
        } else {
            ds = mdb.loadQuery("select cls from ClsFactorVal cl, Cls c where factorVal in (" + fvs + ") and cls<>:cls and " +
                            "cl.cls=c.id and c.typ=" + typ + " group by cls having count(*)=:cnt and count(*)=(select count(*) from ClsFactorVal where cls=cl.cls group by cls)",
                    Map.of("fvs", fvs, "cls", cls, "cnt", cnt));
        }

        if (ds.size() > 0) {
            Store st = mdb.loadQuery("""
                select cod, name from Cls c, ClsVer v, TableLang l
                where c.id=v.ownerVer and v.lastVer=1 and c.id=:cls
                    and l.nameTable='ClsVer' and l.idTable=v.id and l.lang=:lang
            """, Map.of("cls", ds.get(0).getLong("cls"), "lang", lang));
            String msg = "Существует класс, образованный от указанных значений кластерных факторов: ";//NLS
            throw new XError(msg + "[" + st.get(0).get("cod") + ", " + st.get(0).get("name") + "]");
        }

    }

    public Store updateCls(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        String lang = UtCnv.toString(rec.get("lang"));
        long cls = UtCnv.toLong(rec.get("id"));
        long typ = UtCnv.toLong(rec.get("typ"));
        List<String> fvs = UtCnv.toList(params.get("ids"));
        // Проверка уаказанных значении классов на повторность
        int cnt = fvs.size();
        String whFvs = fvs.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(",", "", ""));
        //
        testForDubleCls(whFvs, cnt, cls, typ, "upd", lang);
        //
        Store stDate = mdb.loadQuery("select dbeg, dend from TypVer where ownerVer=:typ and lastVer=1",
                Map.of("typ", typ));
        String d1 = stDate.get(0).get("dbeg") != null ? stDate.get(0).getString("dbeg") : "1800-01-01";
        String d2 = stDate.get(0).get("dend") != null ? stDate.get(0).getString("dend") : "3333-12-31";
        rec.put("dbeg", d1);
        rec.put("dend", d2);
        //upd Cls
        updateEntity(rec);

        Store dsOld = mdb.loadQuery("select id,factorVal from ClsFactorVal where cls=:cls", Map.of("cls", cls));
        Set<Object> oldFvs = dsOld.getUniqueValues("factorVal");
        // Deleting
        for (StoreRecord r : dsOld) {
            if (!fvs.contains(r.getString("factorVal"))) {
                System.out.println("del " + r.getString("id"));
                mdb.execQuery("delete from ClsFactorVal where id=:id", Map.of("id", r.getLong("id")));
            }
        }
        //Adding
        for (String sfv : fvs) {
            long f = UtCnv.toLong(sfv);
            if (!oldFvs.contains(f)) {
                mdb.insertRec("ClsFactorVal", Map.of("cls", cls, "factorVal", f), true);
            }
        }
        //
        return loadRecCls(cls, lang);
    }


    protected void checkExistOwners(long cls, String modelMeta) {
        List<String> lstApp = new ArrayList<>();
        if (modelMeta.equalsIgnoreCase("fish")) {
            //1
            boolean b = apiUserData().get(ApiUserData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("userdata");
            //2
            b = apiNSIData().get(ApiNSIData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("nsidata");
            //3
            b = apiMonitoringData().get(ApiMonitoringData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("monitoringdata");
        }

        if (modelMeta.equalsIgnoreCase("dtj")) {
            boolean b = apiUserData().get(ApiUserData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("userdata");
            b = apiNSIData().get(ApiNSIData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("nsidata");
            b = apiOrgStructureData().get(ApiOrgStructureData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("orgstructuredata");
            b = apiObjectData().get(ApiObjectData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("objectdata");
            b = apiPersonnalData().get(ApiPersonnalData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("personnaldata");
            b = apiPlanData().get(ApiPlanData.class).checkExistOwners(cls, true);
            if (b) lstApp.add("plandata");
        }

        if (!lstApp.isEmpty()) {
            throw new XError("ExistObjInApp@"+UtString.join(lstApp, ", "));
        }

    }

    public void deleteCls(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        long cls = UtCnv.toLong(rec.get("ent"));

        //---< check data in other DB
        CfgService cfgSvc = mdb.getApp().bean(CfgService.class);
        String modelMeta = cfgSvc.getConf().getString("dbsource/default/id");
        if (modelMeta.isEmpty())
            throw new XError("Не найден id мета модели");

        checkExistOwners(cls, modelMeta);
        //--->

        // delete ClsFactorVal
        mdb.execQuery("""
                    delete from ClsFactorVal where cls=:cls
                """, Map.of("cls", cls));

        // delete Cls
        rec.put("id", cls);
        deleteEntity(rec);
    }

    //todo Multu-Lang
    public Store loadClsTreeForSelect() throws Exception {
        String sql = """
                    select -t.id as id, v.name, null as parent
                    from Typ t, TypVer v where t.id=v.ownerVer and v.lastVer=1
                    union all
                    select t.id, v.name, -t.typ as parent
                    from Cls t, ClsVer v where t.id=v.ownerVer and v.lastVer=1
                """;
        return mdb.loadQuery(sql);
    }

    public Store loadClsVer(long cls, String lang) throws Exception {
        Store st = mdb.createStore("ClsVer.lang");
        mdb.loadQuery(st, """
            select * from
            Cls c, ClsVer v, TableLang l
            where c.id=:id and c.id=v.ownerVer and v.lastVer=1
                and l.nameTable='ClsVer' and l.idTable=v.id and l.lang=:lang
            order by dend desc
        """, Map.of("id", cls, "lang", lang));
        return st;
    }

    public Store loadClsVerRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("ClsVer.lang");
        mdb.loadQuery(st, """
            select *
            from ClsVer v
                left join TableLang l on l.nameTable='ClsVer' and l.idTable=v.id and l.lang=:lang
            where v.id=:id
         """, Map.of("id", id, "lang", lang));
        return st;
    }

    public Store insertClsVer(Map<String, Object> rec) throws Exception {
        //
        long id = insertEntityVer(rec);
        //
        return loadClsVerRec(id, UtCnv.toString(rec.get("lang")));
    }

    public Store updateClsVer(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntityVer(rec);
        // Загрузка записи
        return loadClsVerRec(id, UtCnv.toString(rec.get("lang")));
    }

    public void deleteClsVer(Map<String, Object> rec) throws Exception {
        deleteEntityVer(rec);

    }



}
