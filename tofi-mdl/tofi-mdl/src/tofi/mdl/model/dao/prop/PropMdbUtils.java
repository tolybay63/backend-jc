package tofi.mdl.model.dao.prop;

import jandcode.commons.UtCnv;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.commons.variant.IVariantMap;
import jandcode.commons.variant.VariantMap;
import jandcode.core.dbm.dict.DictService;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.std.CfgService;
import jandcode.core.store.Store;
import jandcode.core.store.StoreIndex;
import jandcode.core.store.StoreRecord;
import tofi.api.dta.*;
import tofi.apinator.ApinatorApi;
import tofi.apinator.ApinatorService;
import tofi.mdl.consts.*;
import tofi.mdl.model.utils.EntityConst;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtMeterSoft;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class PropMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;
    ApinatorService apiSrv;

    public PropMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
        //
        apiSrv = mdb.getApp().bean(ApinatorService.class);
    }



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




    public Store loadPropTree(long propGr) throws Exception {
        Store st = mdb.createStore("Prop.rec");
        mdb.loadQuery(st, """
                    select p.*, a.attribvaltype, ac.entitytype, m.meterStruct
                    from Prop p
                        left join attrib a on a.id=p.attrib
                        left join attribchar ac on ac.attrib=a.id
                        left join Meter m on p.meter=m.id
                    where p.propGr=:g
                """, Map.of("g", propGr));
        return st;
    }

    public Store loadRec(long id) throws Exception {
        Store st = mdb.createStore("Prop.rec");
        mdb.loadQuery(st, """
                    select coalesce (p.measure, m.measure) as measure, p.*, a.attribvaltype, ac.entitytype, m.meterStruct,
                        case when prn.proptype is null then 0 else prn.proptype end as parentPropType
                    from Prop p
                        left join attrib a on a.id=p.attrib
                        left join attribchar ac on ac.attrib=a.id
                        left join Meter m on p.meter=m.id
                        left join Prop prn on p.parent=prn.id
                    where p.id=:id
                """, Map.of("id", id));

        mdb.resolveDicts(st);
        return st;
    }

    public StoreRecord newRec(long propGroup) throws Exception {
        Store st = mdb.createStore("Prop");
        StoreRecord r = st.add();
        r.set("propGr", propGroup);
        r.set("propType", FD_PropType_consts.factor);
        r.set("isDependValueOnPeriod", true);
        r.set("isDependNameOnPeriod", false);
        r.set("accessLevel", FD_AccessLevel_consts.common);
        r.set("isUniq", true);
        return r;
    }

    protected void allItemAdd(long id, Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("PropVal");
        if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.factor) {
            Store stFV = mdb.loadQuery("select id from Factor where parent=:factor",
                    Map.of("factor", UtCnv.toLong(rec.get("factor"))));
            for (StoreRecord r : stFV) {
                StoreRecord record = st.add();
                record.set("prop", id);
                record.set("factorVal", r.getLong("id"));
                mdb.insertRec("PropVal", record, true);
            }
        } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.typ) {
            Store stCls = mdb.loadQuery("select id from Cls where typ=:typ",
                    Map.of("typ", UtCnv.toLong(rec.get("typ"))));
            for (StoreRecord r : stCls) {
                StoreRecord record = st.add();
                record.set("prop", id);
                record.set("cls", r.getLong("id"));
                mdb.insertRec("PropVal", record, true);
            }

/*
            List<Map<String, Object>> lst = UtJson.fromJson(UtCnv.toString(rec.get("notCls")), List.class);
            List<Object> arr = new ArrayList<>();
            if (lst != null)
                for (Map<String, Object> map : lst) {
                    arr.add(UtCnv.toLong(map.get("id")));
                }
            String wheCls = String.join(",", UtCnv.toString(arr));
            wheCls = wheCls.replace("[", "(").replace("]", ")");
            if (wheCls.equals("()")) wheCls = "(0)";

            Store stObj = mdb.loadQuery("""
                           select id
                           from Obj where cls in (select id from Cls where typ=:typ)
                           except
                           select id from Obj where cls in
                    """ + wheCls, Map.of("typ", UtCnv.toLong(rec.get("typ")), "prop", id));
            for (StoreRecord r : stObj) {
                StoreRecord record = st.add();
                record.set("prop", id);
                record.set("obj", r.getLong("id"));
                mdb.insertRec("PropVal", record, true);
            }
*/

        } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.reltyp) {
            Store stRelCls = mdb.loadQuery("select id from RelCls where reltyp=:reltyp",
                    Map.of("reltyp", UtCnv.toLong(rec.get("reltyp"))));
            for (StoreRecord r : stRelCls) {
                StoreRecord record = st.add();
                record.set("prop", id);
                record.set("relcls", r.getLong("id"));
                mdb.insertRec("PropVal", record, true);
            }

/*
            Store stRO = mdb.loadQuery("select id from RelObj where relTyp=:rt",
                    Map.of("rt", UtCnv.toLong(rec.get("relTyp"))));
            for (StoreRecord r : stRO) {
                StoreRecord record = st.add();
                record.set("prop", id);
                record.set("relObj", r.getLong("id"));
                mdb.insertRec("PropVal", record, true);
            }
*/
        }
    }

    protected void checkStructComplexProp(Map<String, Object> rec) throws Exception {
        long prop = UtCnv.toLong(rec.get("parent"))>0 ? UtCnv.toLong(rec.get("parent")) : UtCnv.toLong(rec.get("id"));

        //todo Запрос ко всем сервисам данных (использовать Pulsar)

        //
        CfgService cfgSvc = mdb.getApp().bean(CfgService.class);
        String modelMeta = cfgSvc.getConf().getString("dbsource/default/id");
        if (modelMeta.isEmpty())
            throw new XError("Не найден id мета модели");
        //

        if (modelMeta.equalsIgnoreCase("kpi")) {
            Store st = apiUserData().get(ApiUserData.class).loadSql("""
                        select v.id from dataprop d, Datapropval v
                        where d.id=v.dataprop and d.prop=
                    """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@userdata");

        }

        if (modelMeta.equalsIgnoreCase("fish")) {
            Store st = apiUserData().get(ApiUserData.class).loadSql("""
                                select v.id from dataprop d, Datapropval v
                                where d.id=v.dataprop and d.prop=
                            """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@userdata");

            st = apiNSIData().get(ApiNSIData.class).loadSql("""
                            select v.id from dataprop d, Datapropval v
                            where d.id=v.dataprop and d.prop=
                        """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@nsidata");

            st = apiMonitoringData().get(ApiMonitoringData.class).loadSql("""
                                select v.id from dataprop d, Datapropval v
                                where d.id=v.dataprop and d.prop=
                            """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@monitoringdata");
        }

        if (modelMeta.equalsIgnoreCase("dtj")) {
            Store st = apiUserData().get(ApiUserData.class).loadSql("""
                                select v.id from dataprop d, Datapropval v
                                where d.id=v.dataprop and d.prop=
                            """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@userdata");

            st = apiNSIData().get(ApiNSIData.class).loadSql("""
                            select v.id from dataprop d, Datapropval v
                            where d.id=v.dataprop and d.prop=
                        """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@nsidata");

            st = apiObjectData().get(ApiObjectData.class).loadSql("""
                            select v.id from dataprop d, Datapropval v
                            where d.id=v.dataprop and d.prop=
                        """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@objectdata");

            st = apiOrgStructureData().get(ApiOrgStructureData.class).loadSql("""
                            select v.id from dataprop d, Datapropval v
                            where d.id=v.dataprop and d.prop=
                        """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@orgstructuredata");

            st = apiPersonnalData().get(ApiPersonnalData.class).loadSql("""
                            select v.id from dataprop d, Datapropval v
                            where d.id=v.dataprop and d.prop=
                        """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@personnaldata");

            st = apiPlanData().get(ApiPlanData.class).loadSql("""
                            select v.id from dataprop d, Datapropval v
                            where d.id=v.dataprop and d.prop=
                        """ + prop, "");
            if (st.size() > 0)
                throw new XError("NotChangeStructComplexProp@plandata");
        }

    }

    private void addToCharGr(Map<String, Object> rec) throws Exception {
        Store stGr = mdb.loadQuery("select * from typchargrprop where prop="+rec.get("id"));
        if (stGr.size() > 0) {
            StoreRecord gr = stGr.get(0);
            gr.set("prop", rec.get("parent"));
            mdb.insertRec("TypCharGrProp", gr, true);
        }
        //
        stGr = mdb.loadQuery("select * from reltypchargrprop where prop="+rec.get("id"));
        if (stGr.size() > 0) {
            StoreRecord gr = stGr.get(0);
            gr.set("prop", rec.get("parent"));
            mdb.insertRec("RelTypCharGrProp", gr, true);
        }
    }

    public Store insert(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));

        if (UtCnv.toLong(rec.get("parent")) > 0) {
            checkStructComplexProp(params);
            addToCharGr(rec);
        }

        long id = insertEntity(rec);
        //----------------------------------------------------
        if (UtCnv.toBoolean(rec.get("allItem"))) {
            allItemAdd(id, rec);
        }

        if (UtCnv.toBoolean(rec.get("isDependValueOnPeriod"))) {
            mdb.insertRec("PropPeriodType", Map.of("prop", id, "periodType", FD_PeriodType_consts.year));
        }

        //----------------------------------------------------
        Store st = mdb.createStore("Prop");
        mdb.loadQuery(st, """
                    select * from Prop t where t.id=:id
                """, Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    protected void validateUpd(Map<String, Object> rec) throws Exception {
        StoreRecord recOld = loadRec(UtCnv.toLong(rec.get("id"))).get(0);
        //
        haveData(recOld.getValues(), rec);
        //
        long prop = UtCnv.toLong(rec.get("id"));
        if (UtCnv.toLong(rec.get("statusFactor")) == 0) {
            Store tmp = mdb.loadQuery("select id from PropStatus where prop=:p",
                    Map.of("p", prop));
            if (tmp.size() > 0)
                throw new XError("Существует статус данных...");
        }
        if (UtCnv.toLong(rec.get("providerTyp")) == 0) {
            Store tmp = mdb.loadQuery("select id from PropProvider where prop=:p",
                    Map.of("p", prop));
            if (tmp.size() > 0)
                throw new XError("Существует поставщик данных...");
        }
        //

        //allItem true => allItem false
        if (recOld.getBoolean("allItem") && !UtCnv.toBoolean(rec.get("allItem"))) {
            mdb.execQuery("delete from PropVal where prop=:p", Map.of("p", prop));
        }
        //allItem false => allItem true
        if (!recOld.getBoolean("allItem") && UtCnv.toBoolean(rec.get("allItem"))) {
            Store st = mdb.createStore("PropVal");
            if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.factor) {
                Store stFV = mdb.loadQuery("""
                                select id from Factor where parent=:factor
                                except
                                select factorVal as id from PropVal
                                where prop=:prop and factorVal is not null
                        """, Map.of("factor", UtCnv.toLong(rec.get("factor")), "prop", prop));
                for (StoreRecord r : stFV) {
                    StoreRecord record = st.add();
                    record.set("prop", prop);
                    record.set("factorVal", r.getLong("id"));
                    mdb.insertRec("PropVal", record, true);
                }
            } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.typ) {
/*                List<Map<String, Object>> lst = UtJson.fromJson(UtCnv.toString(rec.get("notCls")), List.class);
                List<Object> arr = new ArrayList<>();
                if (lst != null) {
                    for (Map<String, Object> map : lst) {
                        arr.add(UtCnv.toLong(map.get("id")));
                    }
                }
                String wheCls = "(" + UtString.join(arr, ",") + ")";
                if (wheCls.equals("()")) wheCls = "(0)";*/
                Store stCls = mdb.loadQuery("""
                            select id
                            from Cls where typ=:typ
                            except
                            select cls as id from PropVal
                            where prop=:prop and cls is not null
                        """, Map.of("typ", UtCnv.toLong(rec.get("typ")), "prop", prop));
                for (StoreRecord r : stCls) {
                    StoreRecord record = st.add();
                    record.set("prop", prop);
                    record.set("cls", r.getLong("id"));
                    mdb.insertRec("PropVal", record, true);
                }
            } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.reltyp) {
                Store stRC = mdb.loadQuery("""
                                select id from RelCls where relTyp=:rt
                                except
                                select relCls as id from PropVal where prop=:prop and relcls is not null
                        """, Map.of("rt", UtCnv.toLong(rec.get("relTyp")), "prop", prop));
                for (StoreRecord r : stRC) {
                    StoreRecord record = st.add();
                    record.set("prop", prop);
                    record.set("relCls", r.getLong("id"));
                    mdb.insertRec("PropVal", record, true);
                }
            }
        }

        //changed notCls
        /*if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.typ) {
            if (recOld.getBoolean("allItem") && UtCnv.toBoolean(rec.get("allItem"))) {
                if (!UtCnv.toString(rec.get("notCls")).equals(UtCnv.toString(recOld.get("notCls")))) {
                    mdb.execQuery("delete from PropVal where prop=:p", Map.of("p", prop));

                    List<Map<String, Object>> lst = UtJson.fromJson(UtCnv.toString(rec.get("notCls")), List.class);
                    List<Object> arr = new ArrayList<>();
                    if (lst != null) {
                        for (Map<String, Object> map : lst) {
                            arr.add(UtCnv.toLong(map.get("id")));
                        }
                    }
                    String wheCls = "(" + UtString.join(arr, ",") + ")";
                    if (wheCls.equals("()"))
                        wheCls = "(0)";
                    Store stObj = mdb.loadQuery("""
                                select id
                                from Obj where cls in (select id from Cls where typ=:typ)
                                except
                                select id from Obj where cls in
                            """ + wheCls, Map.of("typ", UtCnv.toLong(rec.get("typ")), "prop", prop));
                    for (StoreRecord r : stObj) {
                        mdb.insertRec("PropVal", Map.of("prop", prop, "obj", r.getLong("id")), true);
                    }
                }
            }
        }*/

        //isDependValueOnPeriod false => isDependValueOnPeriod true
        if (!recOld.getBoolean("isDependValueOnPeriod") && UtCnv.toBoolean(rec.get("isDependValueOnPeriod"))) {
            mdb.insertRec("PropPeriodType", Map.of("prop", prop, "periodType", FD_PeriodType_consts.year));
        }
        //isDependValueOnPeriod true => isDependValueOnPeriod false
        if (recOld.getBoolean("isDependValueOnPeriod") && !UtCnv.toBoolean(rec.get("isDependValueOnPeriod"))) {
            mdb.execQuery("""
                        delete from PropPeriodType where prop=:p;
                        delete from PropNameOnPeriod where prop=:p;
                    """, Map.of("p", prop));
        }

        Map<String, Object> mapChanged = new HashMap<>();
        if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.meter && rec.get("parent") == null) {
            if (recOld.getLong("meterBehavior") != UtCnv.toLong(rec.get("meterBehavior"))) {
                mapChanged.put("meterBehavior", UtCnv.toLong(rec.get("meterBehavior")));
            }
            if (recOld.getLong("measure") != UtCnv.toLong(rec.get("measure"))) {
                mapChanged.put("measure", UtCnv.toLong(rec.get("measure")));
            }
            if (recOld.getDouble("minVal") != UtCnv.toDouble(rec.get("minVal"))) {
                mapChanged.put("minVal", UtCnv.toLong(rec.get("minVal")));
            }
            if (recOld.getDouble("maxVal") != UtCnv.toDouble(rec.get("maxVal"))) {
                mapChanged.put("maxVal", UtCnv.toLong(rec.get("maxVal")));
            }
            if (recOld.getInt("digit") != UtCnv.toInt(rec.get("digit"))) {
                mapChanged.put("digit", UtCnv.toLong(rec.get("digit")));
            }

            Store tmp = mdb.loadQuery("""
                        WITH RECURSIVE r AS (
                            select p.id, p.parent from Prop p
                            where p.id=:prop
                            union
                            select p.id, p.parent from Prop p
                                join r on p.parent=r.id
                        ),
                        p as (
                            SELECT * FROM r where r.id <> :prop
                        )
                        select id from Prop p
                        where p.id in (select id from p)
                    """, Map.of("prop", UtCnv.toLong(rec.get("id"))));

            if (!mapChanged.isEmpty())
                for (StoreRecord record : tmp) {
                    mapChanged.put("id", record.getLong("id"));
                    mdb.updateRec("Prop", mapChanged);
                }
        }


        //todo isUniq false => isUniq true
    }

    protected void haveData(Map<String, Object> oldRec, Map<String, Object> newRec) throws Exception {
        IVariantMap oldMap = new VariantMap(oldRec);
        IVariantMap newMap = new VariantMap(newRec);

        // TODO: 02.04.2024 запрос на данные
/*
        String whe = " and d.periodType is null";
        if (oldMap.getBoolean("isDependValueOnPeriod"))
            whe = " and d.periodType is not null";
        if (oldMap.getLong("statusFactor") > 0)
            whe += " and d.status is not null";
        else
            whe += " and d.status is null";
        if (oldMap.getLong("providerTyp") > 0)
            whe += " and d.provider is not null";
        else
            whe += " and d.provider is null";
        String sql = """
                    select distinct
                        case when d.isObj=1 then o.cod else ro.cod end as cod,
                        case when d.isObj=1 then ov.name else rv.name end as name
                    from DataProp d left join DataPropVal v on d.id=v.dataProp
                        left join Obj o on d.isObj=1 and d.owner =o.id
                        inner join ObjVer ov on o.id=ov.ownerVer and ov.lastVer=1
                        left join RelObj ro on d.isObj=0 and d.owner =ro.id
                        inner join RelObjVer rv on rv.id=rv.ownerVer and rv.lastVer=1
                    where d.prop=:prop
                """ + whe + " limit 1";

        if (oldMap.getInt("isDependValueOnPeriod") != newMap.getInt("isDependValueOnPeriod") ||
                oldMap.getLong("statusFactor") != newMap.getLong("statusFactor") ||
                oldMap.getLong("providerTyp") != newMap.getLong("providerTyp") ||
                (!oldMap.getBoolean("isUniq") && newMap.getBoolean("isUniq"))
        ) {
            Store stTmp = mdb.loadQuery(sql, Map.of("prop", oldMap.getLong("id")));
            if (stTmp.size() > 0) {
                String msg = stTmp.get(0).getString("cod") + "-" + stTmp.get(0).getString("name");
                throw new XError("Существуют данные. Например, владелец:\n" + msg);
            }

        }
*/

    }

    public Store update(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        // Validate
        validateUpd(rec);

        //
        //updateEntityWithVer(rec);
        if (UtCnv.toLong(rec.get("parent")) == 0) rec.put("parent", null);
        if (UtCnv.toLong(rec.get("attrib")) == 0) rec.put("attrib", null);
        if (UtCnv.toLong(rec.get("factor")) == 0) rec.put("factor", null);
        if (UtCnv.toLong(rec.get("typ")) == 0) rec.put("typ", null);
        if (UtCnv.toLong(rec.get("relTyp")) == 0) rec.put("relTyp", null);
        if (UtCnv.toLong(rec.get("meter")) == 0) rec.put("meter", null);
        if (UtCnv.toLong(rec.get("meterRate")) == 0) rec.put("meterRate", null);
        if (UtCnv.toLong(rec.get("statusFactor")) == 0) rec.put("statusFactor", null);
        if (UtCnv.toLong(rec.get("providerTyp")) == 0) rec.put("providerTyp", null);
        if (UtCnv.toLong(rec.get("visualFormat")) == 0) rec.put("visualFormat", null);


        updateEntity(rec);
        // Загрузка записи
        Store st = mdb.createStore("Prop");
        mdb.loadQuery(st, """
                    select * from Prop t where t.id=:id
                """, Map.of("id", id));
        mdb.resolveDicts(st);
        return st;
    }

    protected void deleteFromCharGr(Map<String, Object> rec) throws Exception {
        if (UtCnv.toLong(rec.get("parent")) > 0) {
            mdb.execQuery("""
                delete from TypCharGrProp where prop=:parent;
                delete from RelTypCharGrProp where prop=:parent;
            """, rec);
        } else {
            mdb.execQuery("""
                delete from TypCharGrProp where prop in (
                    select id from Prop where parent=:id
                );
                delete from RelTypCharGrProp where prop in (
                    select id from Prop where parent=:id
                );
            """, rec);
        }
    }

    public void delete(Map<String, Object> rec) throws Exception {
        if (UtCnv.toLong(rec.get("propType")) ==FD_PropType_consts.complex ||
                UtCnv.toLong(rec.get("parent")) > 0) {
            checkStructComplexProp(rec);
            if (UtCnv.toBoolean(rec.get("isDependValueOnPeriod"))) {
                mdb.execQuery("""
                    delete from PropPeriodType where prop=:p;
                    delete from PropVal where prop=:p;
                    delete from TypCharGrProp where prop=:p;
                    delete from TypCharGr where id in (
                        select id from TypCharGr where 0=0
                        except
                        select typCharGr as id from TypCharGrProp where 0=0
                    );
                """, Map.of("p", UtCnv.toLong(rec.get("id"))));
            }
            deleteFromCharGr(rec);
        }
        deleteEntity(rec);
    }

    public boolean checkStatus(long prop) throws Exception {
        Store st = mdb.loadQuery("""
                          select id from PropStatus where prop=:p
                """, Map.of("p", prop));
        return st.size() > 0;
    }

    public boolean checkProvider(long prop) throws Exception {
        Store st = mdb.loadQuery("""
                          select id from PropProvider where prop=:p
                """, Map.of("p", prop));
        return st.size() > 0;
    }

    public Store loadStatus(long fk) throws Exception {
        Store st = mdb.createStore("PropStatus.full");
        mdb.loadQuery(st, """
                    select p.*, f.name, f.cod
                    from PropStatus p
                        left join Factor f on f.parent is not null and p.factorval=f.id
                    where prop=:p
                    order by f.ord
                """, Map.of("p", fk));

        return st;
    }

    public Store loadStatusForUpd(long prop, long factor) throws Exception {
        Store st = mdb.createStore("PropStatus.full");
        mdb.loadQuery(st, """
                    select p.id, f.cod, f.name, p.prop, f.id as factorval, p.isdefault,
                        case when p.factorval is not null then 1 else null end as checked
                    from Factor f
                        left join PropStatus p on p.prop=:p and f.id=p.factorval
                    where f.parent=:f
                    order by f.ord
                """, Map.of("p", prop, "f", factor));

        return st;
    }

    public String saveStatus(long prop, List<Map<String, Object>> params) throws Exception {
        //Old values
        Store stOld = mdb.createStore("PropStatus.full");
        mdb.loadQuery(stOld, """
                    select p.id, p.factorVal, f.cod from PropStatus p, Factor f
                    where p.prop=:p and p.factorVal=f.id
                """, Map.of("p", prop));

        Set<Object> oldFvs = stOld.getUniqueValues("factorVal");
        String wheFvs = "(" + UtString.join(oldFvs, ",") + ")";
        if (wheFvs.equals("()")) wheFvs = "(0)";
        //OldFvsData

        //todo Запрос на другую базу
/*
        Store stFvsData = mdb.loadQuery("""
                    select distinct
                    	fv.id, fv.cod
                    from DataProp d, DataPropVal v, PropStatus p, Factor fv
                    where d.prop=:p and d.id=v.dataProp and d.prop=p.prop and p.factorVal=d.status and d.status =fv.id
                    	and d.status  in
                """ + wheFvs, Map.of("p", prop));
        Set<Object> oldStatusData = stFvsData.getUniqueValues("id");    //fvs
        Set<Object> oldIds = stOld.getUniqueValues("id");               //propStatus
*/
        Set<Object> oldStatusData = Set.of(0);    //fvs
        //Set<Object> oldIds = Set.of(0); ???????????????
        Set<Object> oldIds = stOld.getUniqueValues("id");
        //propStatus


        //New values
        Store stNew = mdb.createStore("PropStatus");
        for (Map<String, Object> map : params) {
            StoreRecord r = stNew.add(map);
            r.set("prop", prop);
        }
        Set<Object> newIds = stNew.getUniqueValues("id");

        Set<String> codsFvsForInfo = new HashSet<>();
        //Deleting
        for (StoreRecord r : stOld) {
            if (!newIds.contains(r.getLong("id"))) {
                if (oldStatusData.contains(r.getLong("factorVal"))) {
                    codsFvsForInfo.add(r.getString("cod"));
                } else {
                    try {
                        mdb.deleteRec("PropStatus", r.getLong("id"));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }

        //Saving
        for (StoreRecord r : stNew) {
            if (!oldIds.contains(r.getLong("id"))) {
                mdb.insertRec("PropStatus", r, true);
            } else {
                mdb.updateRec("PropStatus", r);
            }
        }
        //for childs
/*
        Store stChl = mdb.createStore("PropStatus");
        mdb.loadQuery(stChl, "select * from PropStatus where prop=:prop", Map.of("prop", prop));
        Store tmp = mdb.loadQuery("""
                    WITH RECURSIVE r AS (
                        select p.id, p.parent from Prop p
                        where p.id=:prop
                        union
                        select p.id, p.parent from Prop p
                            join r on p.parent=r.id
                    ),
                    p as (
                        SELECT * FROM r where r.id <> :prop
                    )
                    select id from Prop p
                    where p.id in (select id from p)
                """, Map.of("prop", prop));
        for (StoreRecord record : tmp) {
            mdb.execQuery("delete from PropStatus where prop=:prop", Map.of("prop", record.getLong("id")));
            for (StoreRecord rec : stChl) {
                rec.set("prop", record.getLong("id"));
                mdb.insertRec("PropStatus", rec, true);
            }
        }*/
        //
        return UtString.join(codsFvsForInfo, ",");

    }

    //Provider
    public Store loadProvider(long prop) throws Exception {
        Store st = mdb.createStore("PropProvider.full");

        mdb.loadQuery(st, """
            select p.*, v.name as nameCls, '' as nameObj, d.modelname
            from PropProvider p
                left join Cls c on p.cls=c.id
                left join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                left join database d on c."database"=d.id
            where prop=:p
        """, Map.of("p", prop));
        String model = st.get(0).getString("modelname");
        Set<Object> ids = st.getUniqueValues("obj");
        String whe = "(0"+ UtString.join(ids, ",") + ")";
        String sql = "select o.id, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in "+whe;
        Store stObj = sqlLoad(sql, "", model);
        StoreIndex indObj = stObj.getIndex("id");
        for (StoreRecord r : st) {
            StoreRecord rec = indObj.get(r.getLong("obj"));
            if (rec != null) {
                r.set("nameObj", rec.getString("name"));
            }
        }
        return st;
    }

    public void deleteProvider(long id) throws Exception {
        mdb.deleteRec("PropProvider", id);
    }

    public Store loadProviderClsForSelect(long prop, long typ, String mode) throws Exception {

        String whe = "";
/*
        if (mode.equals("ins"))
            whe = " and c.id not in (select cls from PropProvider where prop=:p)";
*/

        String sql = """
            select c.id, name from Cls c, ClsVer v where c.id=v.ownerVer and v.lastVer=1 and c.typ=:t
        """ + whe;

        return mdb.loadQuery(sql, Map.of("t", typ, "p", prop));
    }

    public Store loadProviderObjForSelect(long cls) throws Exception {
        String model = mdb.loadQuery("""
                select modelname from Cls c
                	left join database d on c."database"=d.id
                where c.id=:cls
        """, Map.of("cls", cls)).get(0).getString("modelname");

        String sql = """
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerver and v.lastver=1 and o.cls=
        """+cls;

        return sqlLoad(sql, "", model);
    }

    protected StoreRecord loadProviderRec(long id) throws Exception {
        StoreRecord st = mdb.createStoreRecord("PropProvider.full");

        mdb.loadQueryRecord(st, """
            select p.*, c.name as nameCls, '' as nameObj from PropProvider p
                left join ClsVer c on p.cls=c.ownerVer and c.lastVer=1
            where p.id=:id
        """, Map.of("id", id));

        //todo Запрос к Db data

        return st;
    }

    public StoreRecord saveProvider(Map<String, Object> rec, String mode) throws Exception {
        long id;
        if (mode.equals("ins")) {
            id = mdb.insertRec("PropProvider", rec, true);
        } else {
            id = UtCnv.toLong(rec.get("id"));
            mdb.updateRec("PropProvider", rec);
        }
        //
        return loadProviderRec(id);
    }

    // PropVal
    public Store loadPropPeriodType(long prop) throws Exception {
        Store st = mdb.createStore("PropPeriodType");
        return mdb.loadQuery(st, "select * from PropPeriodType where prop=:p", Map.of("p", prop));
    }

    public Store loadPropPeriodTypeForUpd(long prop) throws Exception {
        Store st = mdb.createStore("PropPeriodType.full");
        String sql = """
                    select p.id, p.text as name, t.id as idInTable,
                        case when t.periodType is null then false else true end as checked
                    from FD_PeriodType p
                        left join PropPeriodType t on t.prop=:p and p.id=t.periodType
                    where p.vis=1
                    order by p.ord
                """;
        return mdb.loadQuery(st, sql, Map.of("p", prop));
    }

    private Store sqlLoad(String sql, String domain, String model) throws Exception {
        if (model.equalsIgnoreCase("userdata"))
            return apiUserData().get(ApiUserData.class).loadSql(sql, domain);
        else if (model.equalsIgnoreCase("nsidata"))
            return apiNSIData().get(ApiNSIData.class).loadSql(sql, domain);
        else if (model.equalsIgnoreCase("monitoringdata"))
        return apiMonitoringData().get(ApiMonitoringData.class).loadSql(sql, domain);
            throw new XError("Unknown model ["+model+"]");
    }

    private Map<String, Object> sqlLoadForMap(String sql, String domain, String metaModel) throws Exception {
        //todo Запрос ко всем сервисам данных (использовать Pulsar)
        Map<String, Object> res = new HashMap<>();
        if (metaModel.equalsIgnoreCase("kpi")) {
            Store st = apiUserData().get(ApiUserData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("userdata", st.getUniqueValues("periodType"));
        } else if (metaModel.equalsIgnoreCase("fish")) {
            Store st = apiUserData().get(ApiUserData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("userdata", st.getUniqueValues("periodType"));
            st = apiNSIData().get(ApiNSIData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("nsidata", st.getUniqueValues("periodType"));
            st = apiMonitoringData().get(ApiMonitoringData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("monitoringdata", st.getUniqueValues("periodType"));
        } else if (metaModel.equalsIgnoreCase("dtj")) {
            Store st = apiUserData().get(ApiUserData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("userdata", st.getUniqueValues("periodType"));
            st = apiNSIData().get(ApiNSIData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("nsidata", st.getUniqueValues("periodType"));
            st = apiObjectData().get(ApiObjectData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("objectdata", st.getUniqueValues("periodType"));
            st = apiOrgStructureData().get(ApiOrgStructureData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("orgstructuredata", st.getUniqueValues("periodType"));
            st = apiPersonnalData().get(ApiPersonnalData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("personnaldata", st.getUniqueValues("periodType"));
            st = apiPlanData().get(ApiPlanData.class).loadSql(sql, domain);
            if (st.size() > 0)
                res.put("plandata", st.getUniqueValues("periodType"));

        }
        //
        return res;
    }

    public String savePropPeriodType(long prop, String metaModel, List<Map<String, Object>> params) throws Exception {
        //Old Data
        Store stOld = mdb.loadQuery("select id, periodType from PropPeriodType where prop=:p",
                Map.of("p", prop));
        Set<Object> idsOld = stOld.getUniqueValues("id");
        //period in data
        Map<String, Object> mapPeriodsOldInData = sqlLoadForMap("select periodType from DataProp where prop="+prop, "", metaModel);

        DictService dc = mdb.getModel().bean(DictService.class);
        //New values
        Store stNew = mdb.createStore("PropPeriodType");
        for (Map<String, Object> map : params) {
            StoreRecord r = stNew.add(map);
            r.set("prop", prop);
        }
        Set<Object> idsNew = stNew.getUniqueValues("id");
        Set<Object> codsPeriodForInfo = new HashSet<>();
        AtomicReference<Map<String, Set<Object>>> mapCodsPeriodForInfo = new AtomicReference<>(new HashMap<>());

        //Deleting
        for (StoreRecord r : stOld) {
            if (!idsNew.contains(r.getLong("id"))) {
                boolean fIn = false;
                for (Map.Entry<String, Object> entry : mapPeriodsOldInData.entrySet()) {
                    if (((Set<Object>) entry.getValue()).contains(r.getLong("periodType"))) {
                        fIn = true;
                        codsPeriodForInfo.add(dc.loadDictData("FD_PeriodType")
                                .getData().getById(r.get("periodType")).getString("text"));
                        mapCodsPeriodForInfo.get().put(entry.getKey(), codsPeriodForInfo);
                    }
                }

                if (!fIn) {
                   mdb.deleteRec("PropPeriodType", r.getLong("id"));
                }
            }
        }
        //Save
        for (StoreRecord r : stNew) {
            if (!idsOld.contains(r.getLong("id"))) {
                mdb.insertRec("PropPeriodType", r, true);
            }
        }

        //for childs
        Store stChld = mdb.loadQuery("""
                    WITH RECURSIVE r AS (
                        select p.id, p.parent from Prop p
                        where p.id=:prop
                        union
                        select p.id, p.parent from Prop p
                            join r on p.parent=r.id
                    ),
                    p as (
                        SELECT * FROM r where r.id <> :prop
                    )
                    select p.id from Prop p
                    where p.id in (select id from p)
                """, Map.of("prop", prop));
        for (StoreRecord record : stChld) {

            stOld = mdb.loadQuery("select id, periodType from PropPeriodType where prop=:p",
                    Map.of("p", record.getLong("id")));
            idsOld = stOld.getUniqueValues("id");


            Set<Object> periodsOldInData = new HashSet<>();

            Map<String, Object> mapTmp = sqlLoadForMap("select periodType from DataProp where prop="+prop, "", metaModel);
            for (Map.Entry<String, Object> entry : mapTmp.entrySet()) {
                periodsOldInData.addAll((Collection<?>) entry.getValue());
            }

            //Deleting
            for (StoreRecord r : stOld) {
                if (!idsNew.contains(r.getLong("id"))) {
                    if (!periodsOldInData.contains(r.getLong("periodType"))) {
                        mdb.deleteRec("PropPeriodType", r.getLong("id"));
                    }
                }
            }
            //Save
            for (StoreRecord r : stNew) {
                if (!idsOld.contains(r.getLong("id"))) {
                    r.set("prop", record.getLong("id"));
                    mdb.insertRec("PropPeriodType", r, true);
                }
            }
        }
        //
        return UtString.join(codsPeriodForInfo, ",");
    }

    public Store loadPropVal(long prop, String entity) throws Exception {
        Store st = mdb.createStore("PropVal.full");
        String sql = switch (entity) {
            case "Factor" -> """
                        select p.*, f.cod, f.name, f.fullName
                        from PropVal p, Factor f
                        where p.prop=:p and p.factorVal=f.id
                    """;
            case "Typ" -> """
                        select p.*, o.cod, v.name, v.fullName
                        from PropVal p, Cls o, ClsVer v
                        where p.prop=:p and p.cls=o.id and o.id=v.ownerVer and v.lastVer=1
                    """;
            case "RelTyp" -> """
                        select p.*, o.cod, v.name, v.fullName
                        from PropVal p, RelCls o, RelClsVer v
                        where p.prop=:p and p.relCls=o.id and o.id=v.ownerVer and v.lastVer=1
                    """;
            case "Measure" -> """
                        select m.id, m.cod, m.name, m.fullName, m.parent
                        from PropVal p, Measure m
                        where p.prop=:p and p.measure=m.id
                    """;
            default -> "";
        };

        mdb.loadQuery(st, sql, Map.of("p", prop));

        //mdb.outTable(st);
        return st;
    }

    public Store loadPropValForUpd(long prop) throws Exception {
        StoreRecord rec = loadRec(prop).get(0);
        String entity = "Factor";
        long entityId = rec.getLong("factor");
        if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.typ) {
            entity = "Typ";
            entityId = rec.getLong("typ");
        } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.reltyp) {
            entity = "RelTyp";
            entityId = rec.getLong("relTyp");
        } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.measure) {
            entity = "Measure";
            entityId = 0;
        }

        Store st = mdb.createStore("PropVal.full");
        String sql;
        //todo left => inner
        if (entity.equals("Factor"))
            sql = """
                        select f.id, f.cod, f.name, f.fullName,
                            case when p.factorVal is null then false else true end as checked
                        from Factor f
                            left join PropVal p on p.prop=:prop and f.id=p.factorVal
                        where f.parent=:ent
                        order by f.ord
                    """;
        else if (entity.equals("Typ")) {

/*            List<Map<String, Object>> lst = UtJson.fromJson(UtCnv.toString(rec.get("notCls")), List.class);
            if (lst == null)
                lst = new ArrayList<>();
            List<Object> arr = new ArrayList<>();
            for (Map<String, Object> map : lst) {
                arr.add(UtCnv.toLong(map.get("id")));
            }
            String wheCls = "(" + UtString.join(arr, ",") + ")";
            if (wheCls.equals("()")) wheCls = "(0)";*/

            sql = """
                    select o.id, o.cod, v.name, v.fullName,
                        case when p.cls is null then false else true end as checked
                    from Cls o left join ClsVer v on o.id=v.ownerVer and v.lastVer=1
                        left join PropVal p on p.prop=:prop and p.cls=o.id
                    where o.id in (select id from Cls where typ=:ent)
                    order by o.ord
            """
            ;
        } else if (entity.equals("RelTyp"))
            sql = """
                        select o.id, o.cod, v.name, v.fullName,
                            case when p.relCls is null then false else true end as checked
                        from RelCls o left join RelClsVer v on o.id=v.ownerVer and v.lastVer=1
                            left join PropVal p on p.prop=:prop and p.relCls=o.id
                        where o.id in (select id from RelCls where reltyp=:ent)
                        order by o.ord
                    """;
        else  {        //entity.equals("Measure")
            sql = """
                        select f.id, f.cod, f.name, f.fullName, f.parent,
                            case when p.measure is null then false else true end as checked
                        from Measure f
                            left join PropVal p on p.prop=:prop and f.id=p.measure
                        where 0=0
                        order by f.id
                    """;
        }

        mdb.loadQuery(st, sql, Map.of("prop", prop, "ent", entityId));
        if (st.size() == 0) {
            throw new XError("notPossibleValue");
        }
        return st;
    }

    public boolean checkRefValue(long prop, String field) throws Exception {
        Store st = mdb.loadQuery("select id from PropVal where prop=:p and "+field+" is not null", Map.of("p", prop));
        return st.size() > 0;
    }

    public void savePropRefVal(long prop, List<Map<String, Object>> params) throws Exception {
        StoreRecord rec = loadRec(prop).get(0);
        String fld = "factorVal";
        if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.typ) {
            fld = "cls";
        } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.reltyp) {
            fld = "relCls";
        } else if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.measure) {
            fld = "measure";
        }

        Set<Object> idsNew = new HashSet<>();
        for (Map<String, Object> map : params) {
            idsNew.add(UtCnv.toLong(map.get("id")));
        }
        Store st = mdb.loadQuery("select " + fld + " as id from PropVal where prop=:p",
                Map.of("p", prop));
        Set<Object> idsOld = st.getUniqueValues("id");
        //Deleting
        for (Object id : idsOld) {
            if (!idsNew.contains(id)) {
                mdb.execQuery("delete from PropVal where prop=:p and " + fld + "=:ent",
                        Map.of("p", prop, "ent", UtCnv.toLong(id)));
            }
        }
        //Saving
        for (Object id : idsNew) {
            if (!idsOld.contains(id)) {
                mdb.insertRec("PropVal", Map.of("prop", prop, fld, UtCnv.toLong(id)), true);
            }
        }
    }

    //---------------------- PropMeter -------------------------------

    public Store loadPropMeter(long prop) throws Exception {
        Store st = mdb.createStore("Prop.meter.full");
        mdb.loadQuery(st, """
                    WITH RECURSIVE r AS (
                        select p.* from Prop p
                        where p.id=:prop
                        union
                        select p.* from Prop p
                            join r on p.parent=r.id
                    ),
                    p as (
                        SELECT * FROM r where r.id <> :prop
                    )
                    select * from Prop p
                    where p.id in (select id from p)
                """, Map.of("prop", prop));

        Stream<StoreRecord> flt = st.getRecords().stream().filter(r -> {
            return r.getLong("parent") == prop;
        });

        flt.forEach(r -> {
            r.set("parent", null);
        });
        //mdb.outTable(st);

        return st;
    }

    public Store loadPropMeterForUpd(Map<String, Object> params) throws Exception {
        long meterStruct = UtCnv.toLong(params.get("meterStruct"));
        if (meterStruct == FD_MeterStruct_consts.soft)
            return loadPropMeterForSoft(params);
        else if (meterStruct == FD_MeterStruct_consts.hard)
            return loadPropMeterForHard(params);
        else throw new XError("meterStruct not defined");
    }

    protected Store loadPropMeterForSoft(Map<String, Object> params) throws Exception {
        long prop = UtCnv.toLong(params.get("prop"));
        long meter = UtCnv.toLong(params.get("meter"));
        long meterRate = UtCnv.toLong(params.get("meterRate"));
        long pt = FD_PropType_consts.rate;

        UtMeterSoft ut = new UtMeterSoft(mdb, meter, false);
        Store stMR = ut.getMeterRatesWithParent();
        StoreIndex indStMR = stMR.getIndex("id");
        //mdb.outTable(stMR);

        String sql = """
                    WITH RECURSIVE r AS (
                        select p.* from Prop p
                        where p.id=:prop
                        union
                        select p.* from Prop p
                            join r on p.parent=r.id
                    ),
                    p as (
                        SELECT * FROM r where r.id <> :prop
                    )
                    select mr.id, mr.parent, pp.accessLevel, :pt as propType, mr.meter, pp.propgr, pp.isdependvalueonperiod,
                        pp.isdependnameonperiod,pp.statusfactor, pp.providertyp, pp.measure, pp.meterbehavior,
                        pp.minval, pp.maxval, pp.digit,
                        pp."name" || ' ' || substring(mr."name" from position( '(' in mr."name")) as name,
                        pp.fullName || ' ' || substring(mr.fullName from position( '(' in mr.fullName)) as fullName,
                        case when p.id is null then false else true end as checked
                    from meterRate mr
                        left join p on p.meterrate=mr.id
                        left join Prop pp on pp.id=:prop and mr.meter=pp.meter
                    where mr.meter=:meter
                    order by mr.ord
                """;

        if (meterRate > 0) {

            Store tmp = mdb.createStore(mdb.createDomain(stMR));
            subTree(stMR, meterRate, tmp);

            String wheMR = "(" + UtString.join(tmp.getUniqueValues("id"), ",") + ")";
            if (wheMR.equals("()"))
                wheMR = "(0)";

            sql = """
                        WITH RECURSIVE r AS (
                            select p.* from Prop p
                            where p.id=:prop
                            union
                            select p.* from Prop p
                                join r on p.parent=r.id
                        ),
                        p as (
                            SELECT * FROM r where r.id <> :prop
                        )
                        select mr.id, case when mr.parent=:mr then null else mr.parent end as parent, pp.accessLevel, :pt as propType,
                        mr.meter, pp.propgr, pp.isdependvalueonperiod, pp.isdependnameonperiod,pp.statusfactor, pp.providertyp,
                            pp.measure, pp.meterbehavior, pp.minval, pp.maxval, pp.digit,
                            pp."name" || ' ' || substring(mr."name" from position( '(' in mr."name")) as name,
                            pp.fullName || ' ' || substring(mr.fullName from position( '(' in mr.fullName)) as fullName,
                            case when p.id is null then false else true end as checked
                        from meterRate mr
                            left join p on p.meterrate=mr.id
                            left join Prop pp on pp.id=:prop and mr.meter=pp.meter
                        where mr.meter=:meter and mr.id in
                    """ + wheMR + " order by mr.ord";
        }


        Store stProp = mdb.createStore("Prop.meter.checked");
        mdb.loadQuery(stProp, sql, Map.of("prop", prop, "meter", meter, "mr", meterRate, "pt", pt));


        for (StoreRecord r : stProp) {
            StoreRecord record = indStMR.get(r.getLong("id"));
            r.set("parent", record.getLong("parent"));
            r.set("meterRate", record.getLong("id"));
        }
        Set<Object> ids = stProp.getUniqueValues("id");

        for (StoreRecord r : stProp) {
            if (!ids.contains(r.getLong("parent")))
                r.set("parent", 0);
        }


        //mdb.outTable(stProp);
        //mdb.outTable(stMR);
        return stProp;
    }

    protected Store loadPropMeterForHard(Map<String, Object> params) throws Exception {
        long prop = UtCnv.toLong(params.get("prop"));
        long meter = UtCnv.toLong(params.get("meter"));
        long meterRate = UtCnv.toLong(params.get("meterRate"));
        Store st = mdb.createStore("Prop.meter.checked");
        long pt = FD_PropType_consts.rate;

        String sql = """
                    WITH RECURSIVE r AS (
                        select p.* from Prop p
                        where p.id=:prop
                        union
                        select p.* from Prop p
                            join r on p.parent=r.id
                    ),
                    p as (
                        SELECT * FROM r where r.id <> :prop
                    )
                    select mr.id, mr.parent, pp.accessLevel, :pt as propType, mr.meter, pp.propgr, pp.isdependvalueonperiod,
                        pp.isdependnameonperiod,pp.statusfactor, pp.providertyp, pp.measure, pp.meterbehavior,
                        pp.minval, pp.maxval, pp.digit,
                        pp."name" || ' (' || mr."name" || ')' as name, pp.fullName || ' (' || mr.fullName || ')' as fullName,
                        case when p.id is null then false else true end as checked
                    from meterRate mr
                        left join p on p.meterrate=mr.id
                        left join Prop pp on pp.id=:prop and mr.meter=pp.meter
                    where mr.meter=:meter
                    order by mr.ord
                """;
        if (meterRate > 0) {
            Store tmp = mdb.loadQuery("""
                        WITH RECURSIVE r AS (
                            select id from meterRate m
                            where m.meter=:m and m.id=:mr
                           union
                            select m.id from meterRate m
                                join r
                                  ON m.parent = r.id
                        )
                        SELECT id FROM r where r.id <> :mr;
                    """, Map.of("m", meter, "mr", meterRate));

            String wheMR = String.join(",", UtCnv.toString(tmp.getUniqueValues("id")));
            wheMR = wheMR.replace("[", "(").replace("]", ")");

            sql = """
                        WITH RECURSIVE r AS (
                            select p.* from Prop p
                            where p.id=:prop
                            union
                            select p.* from Prop p
                                join r on p.parent=r.id
                        ),
                        p as (
                            SELECT * FROM r where r.id <> :prop
                        )
                        select mr.id, case when mr.parent=:mr then null else mr.parent end as parent, pp.accessLevel, :pt as propType,
                        mr.meter, pp.propgr, pp.isdependvalueonperiod, pp.isdependnameonperiod,pp.statusfactor, pp.providertyp,
                            pp.measure, pp.meterbehavior, pp.minval, pp.maxval, pp.digit,
                            pp."name" || ' (' || mr."name" || ')' as name, pp.fullName || ' (' || mr.fullName || ')' as fullName,
                            case when p.id is null then false else true end as checked
                        from meterRate mr
                            left join p on p.meterrate=mr.id
                            left join Prop pp on pp.id=:prop and mr.meter=pp.meter
                        where mr.meter=:meter and mr.id in
                    """ + wheMR + " order by mr.ord";
        }


        mdb.loadQuery(st, sql, Map.of("prop", prop, "meter", meter, "mr", meterRate, "pt", pt));

        //mdb.outTable(st);
        return st;
    }

    //-----------------
    public Store loadPropMeterForUpdSave(Map<String, Object> params) throws Exception {
        long meterStruct = UtCnv.toLong(params.get("meterStruct"));
        if (meterStruct == FD_MeterStruct_consts.soft)
            return loadPropMeterForUpdSaveSoft(params);
        else if (meterStruct == FD_MeterStruct_consts.hard)
            return loadPropMeterForUpdSaveHard(params);
        else throw new XError("meterStruct not defined");
    }

    protected void subTree(Store st, long rootId, Store rez) {
        Stream<StoreRecord> lr = st.getRecords().stream().filter(new Predicate<StoreRecord>() {
            @Override
            public boolean test(StoreRecord r) {
                return r.getLong("parent") == rootId;
            }
        });
        lr.forEach(r -> {
            r.set("cmt", "o");
            rez.add(r);
            subTree(st, r.getLong("id"), rez);
        });
    }

    public Store loadPropMeterForUpdSaveHard(Map<String, Object> params) throws Exception {

        long meter = UtCnv.toLong(params.get("meter"));
        List<Map<String, Object>> lstCheckeds = (List<Map<String, Object>>) params.get("checkeds");
        UtMeterSoft utMS = new UtMeterSoft(mdb, meter);
        //
        Store stPath = mdb.createStore("MeterRate.hard.path");
        mdb.loadQuery(stPath, "select id, parent from MeterRate where meter=:m", Map.of("m", meter));
        utMS.setPath(stPath);

        Store stRez = mdb.createStore("Prop.meter.full");

        Map<Long, Long> mapIdParent = new HashMap<>();
        for (Map<String, Object> map : lstCheckeds) {
            stRez.add(map);
            mapIdParent.put(UtCnv.toLong(map.get("id")), UtCnv.toLong(map.get("parent")));
        }

        //mdb.outTable(stRez);
        StoreIndex indStPath = stPath.getIndex("id");

        for (long id : mapIdParent.keySet()) {
            //mdb.outTable(indStPath.get(id));
            StoreRecord r = indStPath.get(id);
            if (!mapIdParent.containsKey(r.getLong("parent"))) {
                if (r.getString("path").contains(",")) {
                    List<Long> lstPath = utMS.strToList((r.getString("path")));
                    for (Long aLong : lstPath) {
                        boolean ok = false;
                        if (mapIdParent.containsKey(aLong)) {
                            r.set("parent", aLong);
                            ok = true;
                            break;
                        }
                        r.set("parent", 0L);
                    }
                } else {
                    long prt = r.getLong("path");
                    if (prt > 0) {
                        if (!mapIdParent.containsKey(prt))
                            r.set("parent", 0L);
                    }
                }
            }
        }

        for (StoreRecord r : stRez) {
            StoreRecord record = indStPath.get(r.getLong("id"));
            r.set("parent", record.getLong("parent"));
        }

        //mdb.outTable(stPath);
        //mdb.outTable(stRez);

        return stRez;
    }

    public Store loadPropMeterForUpdSaveSoft(Map<String, Object> params) throws Exception {
        //long prop = UtCnv.toLong(params.get("prop"));
        long meter = UtCnv.toLong(params.get("meter"));
        //long meterRate = UtCnv.toLong(params.get("meterRate"));
        List<Map<String, Object>> lstCheckeds = (List<Map<String, Object>>) params.get("checkeds");

        UtMeterSoft utMeterSoft = new UtMeterSoft(mdb, meter, false);
        Store stPath = utMeterSoft.getMeterRatesWithParent();
        utMeterSoft.setPath(stPath);
        //mdb.outTable(stPath);

        Store stRez = mdb.createStore("Prop.meter.full");

        Map<Long, Long> mapIdParent = new HashMap<>();
        for (Map<String, Object> map : lstCheckeds) {
            stRez.add(map);
            mapIdParent.put(UtCnv.toLong(map.get("id")), UtCnv.toLong(map.get("parent")));
        }


        StoreIndex indStPath = stPath.getIndex("id");

        for (long id : mapIdParent.keySet()) {
            //mdb.outTable(indStPath.get(id));
            StoreRecord r = indStPath.get(id);
            if (!mapIdParent.containsKey(r.getLong("parent"))) {
                if (r.getString("path").contains(",")) {
                    List<Long> lstPath = utMeterSoft.strToList((r.getString("path")));
                    for (Long aLong : lstPath) {
                        boolean ok = false;
                        if (mapIdParent.containsKey(aLong)) {
                            r.set("parent", aLong);
                            ok = true;
                            break;
                        }
                        r.set("parent", 0L);
                    }
                } else {
                    long prt = r.getLong("path");
                    if (prt > 0) {
                        if (!mapIdParent.containsKey(prt))
                            r.set("parent", 0L);
                    }
                }
            }
        }

        for (StoreRecord r : stRez) {
            StoreRecord record = indStPath.get(r.getLong("id"));
            r.set("parent", record.getLong("parent"));
        }

        //mdb.outTable(stRez);


        return stRez;
    }

    public void savePropMeter(Map<String, Object> params) throws Exception {
        long meterStruct = UtCnv.toLong(params.get("meterStruct"));
        if (meterStruct == FD_MeterStruct_consts.soft)
            savePropMeterHard(params);
        else if (meterStruct == FD_MeterStruct_consts.hard)
            savePropMeterHard(params);
        else throw new XError("meterStruct not defined");
    }

    protected void savePropMeterHard(Map<String, Object> params) throws Exception {
        long prop = UtCnv.toLong(params.get("prop"));
        //long meter = UtCnv.toLong(params.get("meter"));
        //long meterRate = UtCnv.toLong(params.get("meterRate"));
        List<Map<String, Object>> lstData = (List<Map<String, Object>>) params.get("data");

        Store tmp = mdb.loadQuery("""
                    WITH RECURSIVE r AS (
                        select p.id from Prop p
                        where p.id=:prop
                        union
                        select p.id from Prop p
                            join r on p.parent=r.id
                    ),
                    p as (
                        SELECT * FROM r where r.id <> :prop
                    )
                    select id, meterRate from Prop where id in (select id from p)
                """, Map.of("prop", prop));

        Map<Long, Long> mapOldMR2MP = new LinkedHashMap<Long, Long>();
        for (StoreRecord r : tmp) {
            mapOldMR2MP.put(r.getLong("meterRate"), r.getLong("id"));
        }

        // Real records = old + new
        Set<Long> setNewMR = new LinkedHashSet<>();
        for (Map<String, Object> map : lstData) {
            setNewMR.add(UtCnv.toLong(map.get("id")));
        }

        //Old parents set to null
        String idsPropOld = String.join(",", UtCnv.toString(mapOldMR2MP.values()))
                .replace("[", "(").replace("]", ")");
        if (idsPropOld.equals("()")) idsPropOld = "(0)";
        mdb.execQuery("""
                    update Prop set parent=null
                    where id in
                """ + idsPropOld);
        //Deleting
        Set<Long> ids = new HashSet<Long>();
        for (long key : mapOldMR2MP.keySet()) {
            if (!setNewMR.contains(key)) {
                String sql = "delete from PropPeriodType where prop=:prop" + ";" +
                        "delete from PropNameOnPeriod where prop=:prop" + ";" +
                        "delete from PropStatus where prop=:prop" + ";" +
                        "delete from PropProvider where prop=:prop" + ";" +
                        "delete from Prop where id=:prop" + ";";
                mdb.execQuery(sql, Map.of("prop", mapOldMR2MP.get(key)));
            } else {
                ids.add(mapOldMR2MP.get(key));
            }
        }

        //Define parent
        Map<Long, Long> mapIdParent = new HashMap<>();
        mapIdParent.put(0L, prop);
        for (Map<String, Object> map : lstData) {
            mapIdParent.put(UtCnv.toLong(map.get("id")), UtCnv.toLong(map.get("parent")));
        }

        //Save without parent
        for (Map<String, Object> map : lstData) {
            if (!mapOldMR2MP.containsKey(UtCnv.toLong(map.get("id")))) {

                mdb.outMap(map);

                map.put("meterRate", map.get("id"));
                map.put("id", null);
                map.put("parent", null);
                //long id = insertEntityWithVer(map);
                long id = insertEntity(map);
                ids.add(id);
            }
        }
        //Load
        String wheIds = String.join(",", UtCnv.toString(ids)).replace("[", "").replace("]", "");
        if (wheIds.isBlank()) wheIds = "0";
        Store st = mdb.createStore("Prop.meter");
        mdb.loadQuery(st, "select * from Prop p where p.id in (" + wheIds + ")");

        //mdb.outTable(st);
        //Define parent
        Map<Long, Long> mapIdParentReal = new HashMap<>();
        mapIdParentReal.put(0L, prop);
        for (StoreRecord r : st) {
            mapIdParentReal.put(r.getLong("meterRate"), r.getLong("id"));
        }
        for (StoreRecord r : st) {
            r.set("parent", mapIdParentReal.get(mapIdParent.get(r.getLong("meterRate"))));
            mdb.updateRec("Prop", r);
        }

        //mdb.outTable(st);

    }

    //----------------------------------------------------------------------------------
    public Store loadPropValEntity(long prop, long entityType) throws Exception {
        if (Arrays.asList(FD_EntityType_consts.Factor, FD_EntityType_consts.Type,
                FD_EntityType_consts.RelTyp, FD_EntityType_consts.Measure).contains(entityType)) {
            return loadPropValEntityList(prop, entityType);
        } else if (Arrays.asList(FD_EntityType_consts.FactorVal, FD_EntityType_consts.Cls/*,
                FD_EntityType_consts.Obj, FD_EntityType_consts.RelObj*/).contains(entityType)) {
            return loadPropValEntityTree(prop, entityType);
        } else if (FD_EntityType_consts.Prop == entityType) {
            return loadPropValEntityTreeProp(prop);
        } else {
            throw new XError("Не реализован");
        }
    }

    protected Store loadPropValEntityTreeProp(long prop) throws Exception {
        Store st = mdb.createStore("PropVal.full.tree");
        String sql = """
                    select 'p_'||p.id as id, 'g_'||p.propGr as parent, p.cod, p.name, p.fullName, pv.id as propVal,
                        p.id as prop, p.propGr as propGr, pv.entityId, true as isEntity, p.propType, p.ord
                    from PropVal pv, Prop p
                    where pv.prop=:prop and pv.entityId=p.id
                """;
        mdb.loadQuery(st, sql, Map.of("prop", prop));

        // PropGr
        Store stGr = mdb.createStore("Prop.propGr");
        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr
                    from PropGr where 0=0
                """;
        mdb.loadQuery(stGr, sqlGr);
        StoreIndex indStGr = stGr.getIndex("id");
        //setParents for Meter, Factor... without MeterRate
        Set<Object> idsGr = new HashSet<>();
        for (StoreRecord r1 : st) {
            if (r1.getString("parent").startsWith("p_"))
                continue;
            String curIdgr = "";
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("propGr") == r2.getLong("propGr")) {
                    curIdgr = r2.getString("id");
                    break;
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr);
            if (recGr != null) {
                idsGr.add(recGr.getString("id").substring(2));
                Object parentGr = recGr.get("parent");
                while (parentGr != "") {
                    idsGr.add(UtCnv.toString(parentGr).substring(2));
                    recGr = indStGr.get(parentGr);
                    parentGr = recGr.get("parent");
                }
            }
        }
        //Parents stGr
        String whe = String.join(",", UtCnv.toString(idsGr))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        stGr = mdb.createStore("PropVal.full.tree");
        sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr, cod, name
                    from PropGr where id in
                """ + whe;

        mdb.loadQuery(stGr, sqlGr);
        stGr.add(st);
        //mdb.outTable(stGr);

        Store stGrAll = mdb.createStore("PropVal.full.tree");
        String sqlGrAll = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr,
                    null as prop, null as propType, cod, name
                    from PropGr where 0=0
                    union all
                    select 'p_'||p.id as id,
                    case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent, p.propgr,
                    p.id as prop, p.propType, p.cod, p.name
                    from Prop p
                    where 0=0
                """;
        mdb.loadQuery(stGrAll, sqlGrAll);
        StoreIndex indStAll = stGrAll.getIndex("id");
        //mdb.outTable(stGrAll);

        //Analize parents
        Set<Object> ids = stGr.getUniqueValues("id");
        Set<Object> setPropGr = new HashSet<>();
        Set<Object> setProp = new HashSet<>();
        for (StoreRecord r : stGr) {
            if (r.getString("parent").isBlank())
                continue;
            if (!ids.contains(r.getString("parent"))) {
                setPropGr.add(r.getLong("propGr"));
                setProp.add(r.getLong("prop"));
            }
        }
        if (setProp.isEmpty())
            return stGr;

        String whGr = String.join(",", UtCnv.toString(setPropGr))
                .replace("[", "(").replace("]", ")");
        if (whGr.equals("()")) whGr = "(0)";
        Store stPath = mdb.createStore("DomainPath");
        mdb.loadQuery(stPath, "select id, parent, '' as path from Prop where propGr in " + whGr);
        UtMeterSoft utMeterSoft = new UtMeterSoft(mdb, 0, false);
        utMeterSoft.setPath(stPath);
        //mdb.outTable(stPath);

        StoreIndex indStProp = stPath.getIndex("id");
        StoreIndex indStGrProp = stGr.getIndex("id");

        Store stGrCopy = mdb.createStore("PropVal.full.tree");
        Set<Object> idsPropGrDop = new HashSet<>();

        for (Object it : setPropGr) {
            String gr = "g_" + UtCnv.toString(it);
            for (StoreRecord r : stGr) {
                if (!setProp.contains(r.getLong("prop"))) continue;
                StoreRecord rec = indStProp.get(r.getLong("prop"));
                if (rec != null) {
                    List<String> lstPrts = List.of(rec.getString("path").split(","));
                    for (String el : lstPrts) {
                        boolean ok = false;
                        if (ids.contains("p_" + el)) {
                            r.set("parent", "p_" + el);
                            ok = true;
                            break;
                        }
                        if (!ok) {
                            StoreRecord recGr = indStGrProp.get(gr);
                            if (recGr != null)
                                r.set("parent", recGr.getString("parent"));
                            else {
                                recGr = indStAll.get(gr);
                                while (true) {
                                    if (recGr.getString("id").startsWith("g_"))
                                        break;
                                    recGr = indStAll.get(recGr.getString("parent"));
                                }
                                r.set("parent", recGr.getString("parent"));
                                idsPropGrDop.add(recGr.getLong("propGr"));
                            }
                        }
                    }
                }
            }
        }
        //Defined groups for PropMeter
        whe = String.join(",", UtCnv.toString(idsPropGrDop))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        mdb.loadQuery(stGrCopy, sqlGr);
        //
        stGr.add(stGrCopy);

        //mdb.outTable(stGrCopy);
        //mdb.outTable(stGr);

        return stGr;
    }

    public Store loadPropValEntityTreePropForUpd(long prop) throws Exception {
        //old values
        Store stOld = loadPropValEntityTreeProp(prop);
        StoreIndex indStOld = stOld.getIndex("id");

        Store st = mdb.createStore("PropVal.full.tree");
        String sql = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr,
                    null as prop, null as entityId, null as propType, cod, name, fullName, false as checked
                    from PropGr where 0=0
                    union all
                    select 'p_'||p.id as id,
                    case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent, p.propgr,
                    p.id as prop, p.id as entityId, p.propType, p.cod, p.name, fullName, false as checked
                    from Prop p
                    where 0=0
                """;
        mdb.loadQuery(st, sql);

        //mdb.outTable(st);

        for (StoreRecord r : st) {
            StoreRecord rec = indStOld.get(r.getString("id"));
            if (rec != null) {
                r.set("checked", true);
                r.set("entityId", rec.getLong("entityId"));
            }
        }
        //
        return st;
    }


    ///////
    protected Store loadPropValEntityTree(long prop, long entityType) throws Exception {
        Store st = mdb.createStore("PropVal.full.tree");
        String sql = "";

        if (entityType == FD_EntityType_consts.FactorVal) {
            sql = """
                        select * from (
                            select f.id, null parent, f.cod, f.name, f.fullName, null as prop, null as propVal, null as entityId, f.ord
                            from Factor f
                            where f.id in (
                                select f.parent
                                from PropVal p, Factor f
                                where p.prop=:p and p.entityId=f.id and f.parent is not null
                            )
                            union all
                            select f.id, f.parent, f.cod, f.name, f.fullName, p.prop, p.id as propVal, p.entityId, f.ord
                            from PropVal p, Factor f
                            where p.prop=:p and p.entityId=f.id and f.parent is not null
                        ) t where 0=0
                        order by t.ord
                    """;
        } else if (entityType == FD_EntityType_consts.Cls) {
            sql = """
                        select * from (
                            select 't_'||t.id as id, null parent, t.cod, v.name, v.fullName, null as prop, null as propVal, null as entityId, t.ord
                            from Typ t, TypVer v
                            where t.id in (
                                select c.typ
                                from PropVal p, Cls c
                                where p.prop=:p and p.entityId=c.id
                            ) and t.id=v.ownerVer and v.lastVer=1
                            union all
                            select 'c_' || c.id as id, 't_' || c.typ as parent, c.cod, v.name, v.fullName, p.prop, p.id as propVal, p.entityId, c.ord
                            from PropVal p, Cls c, ClsVer v
                            where p.prop=:p and p.entityId=c.id and c.id=v.ownerVer and v.lastVer=1
                        ) a where 0=0
                        order by a.ord
                    """;
        }/* else if (entityType == FD_EntityType_consts.Obj) {
            sql = """
                        select * from (
                            select 't_'||t.id as id, null parent, t.cod, v.name, v.fullName, null as prop, null as propVal, null as entityId, t.ord
                            from Typ t, TypVer v
                            where t.id in (
                                select c.typ
                                from PropVal p, Obj o, Cls c
                                where p.prop=:p and p.entityId=o.id and c.id=o.cls
                            ) and t.id=v.ownerVer and v.lastVer=1
                            union all
                            select 'o_' || o.id as id, 't_' || c.typ as parent, o.cod, v.name, v.fullName, p.prop, p.id as propVal, p.entityId, o.ord
                            from PropVal p, Obj o, ObjVer v, Cls c
                            where p.prop=:p and p.entityId=o.id and o.id=v.ownerVer and v.lastVer=1 and c.id=o.cls
                        ) a where 0=0
                        order by a.ord
                    """;
        } else if (entityType == FD_EntityType_consts.RelObj) {
            sql = """
                        select * from (
                            select 'r_' || r.id as id, null parent, r.cod, v.name, v.fullName, null as prop, null as propVal, null as entityId, r.ord
                            from RelTyp r, RelTypVer v
                            where r.id in (
                                select o.relTyp
                                from PropVal p, RelObj o
                                where p.prop=:p and p.entityId=o.id
                            ) and r.id=v.ownerVer and v.lastVer=1
                            union all
                            select 'o_' || o.id as id, 'r_' || o.relTyp as parent, o.cod, v.name, v.fullName, p.prop, p.id as propVal, p.entityId, o.ord
                            from PropVal p, RelObj o, RelObjVer v
                            where p.prop=:p and p.entityId=o.id and o.id=v.ownerVer and v.lastVer=1
                        ) a where 0=0
                        order by a.ord
                    """;
        }*/

        mdb.loadQuery(st, sql, Map.of("p", prop));
        return st;
    }

    protected Store loadPropValEntityList(long prop, long entityType) throws Exception {

        Store st = mdb.createStore("PropVal.full");
        String sql = "";
        // for Factor
        if (entityType == FD_EntityType_consts.Factor) {
            sql = """
                        select p.*, f.cod, f.name, f.fullName
                        from PropVal p, Factor f
                        where p.prop=:p and p.entityId=f.id and f.parent is null
                    """;
        } else if (entityType == FD_EntityType_consts.Type) {
            sql = """
                        select p.*, f.cod, v.name, v.fullName
                        from PropVal p, Typ f, TypVer v
                        where p.prop=:p and p.entityId=f.id and f.id=v.ownerVer and v.lastVer=1
                    """;
        } else if (entityType == FD_EntityType_consts.RelTyp) {
            sql = """
                        select p.*, f.cod, v.name, v.fullName
                        from PropVal p, RelTyp f, RelTypVer v
                        where p.prop=:p and p.entityId=f.id and f.id=v.ownerVer and v.lastVer=1
                    """;
        } else if (entityType == FD_EntityType_consts.Measure) {
            sql = """
                        select p.*, f.cod, f.name, f.fullName
                        from PropVal p, Measure f
                        where p.prop=:p and p.entityId=f.id
                    """;
        }

        mdb.loadQuery(st, sql, Map.of("p", prop));
        return st;
    }

    public Store loadPropValEntityForUpd(long prop, long entityType) throws Exception {
        if (Arrays.asList(FD_EntityType_consts.Factor, FD_EntityType_consts.Type,
                FD_EntityType_consts.RelTyp, FD_EntityType_consts.Measure).contains(entityType)) {
            return loadPropValEntityListForUpd(prop, entityType);
        } else if (Arrays.asList(FD_EntityType_consts.FactorVal, FD_EntityType_consts.Cls,
                /*FD_EntityType_consts.RelObj, FD_EntityType_consts.Obj,*/ FD_EntityType_consts.Prop).contains(entityType)) {
            return loadPropValEntityTreeForUpd(prop, entityType);
        } else {
            throw new XError("Не реализован");
        }
    }

    protected Store loadPropValEntityTreeForUpd(long prop, long entityType) throws Exception {
        Store st = mdb.createStore("PropVal.full.tree");
        String sql = "";

        if (entityType == FD_EntityType_consts.FactorVal) {
            sql = """
                        select f.id, f.parent, f.cod, f.name, f.fullName, p.prop, p.id as propVal, f.ord,
                            case when p.id is null then false else true end as checked,
                            case when f.parent is null then null else f.id end as entityId,
                            case when f.parent is null then false else true end as isEntity
                        from Factor f left join PropVal p on f.parent is not null and f.id=p.entityId and p.prop=:p
                        where 0=0
                        order by f.ord
                    """;
        } else if (entityType == FD_EntityType_consts.Cls) {
            sql = """
                        select * from (
                            select 't_'||t.id as id, null parent, t.cod, v.name, v.fullName, null as prop, null as propVal, null as entityId, t.ord,
                                false as checked, false as isEntity
                            from Typ t, TypVer v
                            where t.id=v.ownerVer and v.lastVer=1
                            union all
                            select 'c_' || c.id as id, 't_' || c.typ as parent, c.cod, v.name, v.fullName, p.prop, p.id as propVal, c.id as entityId, c.ord,
                                case when p.id is null then false else true end as checked, true as isEntity
                            from Cls c left join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                                left join PropVal p on c.id=p.entityId and p.prop=:p
                            where  0=0
                        ) a where 0=0
                        order by a.ord
                    """;
        }/* else if (entityType == FD_EntityType_consts.Obj) {
            sql = """
                        select * from (
                            select 't_'||t.id as id, null parent, t.cod, v.name, v.fullName, null as prop, null as propVal, null as entityId, t.ord,
                                false as checked, false as isEntity
                            from Typ t, TypVer v
                            where t.id=v.ownerVer and v.lastVer=1
                            union all
                            select 'o_' || o.id as id, 't_' || c.typ as parent, o.cod, v.name, v.fullName, p.prop, p.id as propVal, o.id as entityId, o.ord,
                                case when p.id is null then false else true end as checked, true as isEntity
                            from Obj o left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                                left join Cls c on c.id=o.cls
                                left join PropVal p on p.prop=:p and p.entityId=o.id
                        ) a where 0=0
                        order by a.ord
                    """;
        } else if (entityType == FD_EntityType_consts.RelObj) {
            sql = """
                        select * from (
                            select 'r_' || r.id as id, null parent, r.cod, v.name, v.fullName, null as prop, null as propVal, null as entityId, r.ord,
                                false as checked, false as isEntity
                            from RelTyp r, RelTypVer v
                            where r.id=v.ownerVer and v.lastVer=1
                            union all
                            select 'o_' || o.id as id, 'r_' || o.relTyp as parent, o.cod, v.name, v.fullName, p.prop, p.id as propVal, o.id as entityId, o.ord,
                                case when p.id is null then false else true end as checked, true as isEntity
                            from  RelObj o left join RelObjVer v on o.id=v.ownerVer and v.lastVer=1
                                left join PropVal p on p.prop=:p and p.entityId=o.id
                        ) a where 0=0
                        order by a.ord
                    """;
        }*/

        mdb.loadQuery(st, sql, Map.of("p", prop));
        //mdb.outTable(st);

        return st;
    }

    protected Store loadPropValEntityListForUpd(long prop, long entityType) throws Exception {
        Store st = mdb.createStore("PropVal.full");

        String sql = "";
        if (entityType == FD_EntityType_consts.Factor) {
            sql = """
                    select p.*, f.id as entId, f.cod, f.name, f.fullName,
                        case when p.id is null then false else true end as checked
                    from Factor f
                        left join PropVal p on p.prop=:p and p.entityId=f.id
                    where f.parent is null
                    """;
        } else if (entityType == FD_EntityType_consts.Type) {
            sql = """
                        select p.*, f.id as entId, f.cod, v.name, v.fullName,
                            case when p.id is null then false else true end as checked
                        from Typ f
                            left join TypVer v on f.id=v.ownerVer and v.lastVer=1
                            left join PropVal p on p.prop=:p and p.entityId=f.id
                        where 0=0
                    """;
        } else if (entityType == FD_EntityType_consts.RelTyp) {
            sql = """
                        select p.*, f.id as entId, f.cod, v.name, v.fullName,
                            case when p.id is null then false else true end as checked
                        from RelTyp f
                            left join RelTypVer v on f.id=v.ownerVer and v.lastVer=1
                            left join PropVal p on p.prop=:p and p.entityId=f.id
                        where 0=0
                    """;
        } else if (entityType == FD_EntityType_consts.Measure) {
            sql = """
                        select p.*, f.id as entId, f.cod, f.name, f.fullName,
                            case when p.id is null then false else true end as checked
                        from Measure f
                            left join PropVal p on p.prop=:p and p.entityId=f.id
                        where 0=0
                    """;
        }

        mdb.loadQuery(st, sql, Map.of("p", prop));
        return st;

    }

    public void savePropEntityVal(long prop, List<Map<String, Object>> lstData) throws Exception {
        StoreRecord rec = loadRec(prop).get(0);
        long entityType = rec.getLong("entityType");
        String fldId = "id";
        if (Arrays.asList(FD_EntityType_consts.FactorVal, FD_EntityType_consts.Cls,
                /*FD_EntityType_consts.RelObj, FD_EntityType_consts.Obj, */FD_EntityType_consts.Prop).contains(entityType)) {
            fldId = "entityId";
        }

        Set<Object> idsNew = new HashSet<>();
        for (Map<String, Object> map : lstData) {
            idsNew.add(UtCnv.toLong(map.get(fldId)));
        }
        Store st = mdb.loadQuery("select entityId as id from PropVal where prop=:p",
                Map.of("p", prop));
        Set<Object> idsOld = st.getUniqueValues("id");

        //Deleting
        for (Object id : idsOld) {
            if (!idsNew.contains(id)) {
                mdb.execQuery("delete from PropVal where prop=:p and entityId=:ent",
                        Map.of("p", prop, "ent", UtCnv.toLong(id)));
            }
        }
        //Saving
        for (Object id : idsNew) {
            if (!idsOld.contains(id)) {
                mdb.insertRec("PropVal", Map.of("prop", prop, "entityId", UtCnv.toLong(id)), true);
            }
        }
    }

    public Store loadPropForSelect(String propField) throws Exception {
        if (propField.equals("prop")) {
            return loadAllPropForSelect();
        } else {
            return loadAllMultiPropForSelect();
        }
    }

    //
    public Store loadAllPropForSelect() throws Exception {
        return mdb.loadQuery("""
                    select 'g_'||id as id, case when parent is not null then 'g_'||parent else null end as parent,
                        -id as prop, name, fullName, 0 as ent
                    from PropGr where 0=0
                    union all
                    select 'p_'||id as id, case when parent is null then 'g_'||propGr else 'p_'||parent end as parent,
                        id as prop, name, fullName, id as ent
                    from Prop where 0=0
                """);
    }

    public Store loadAllMultiPropForSelect() throws Exception {
        return mdb.loadQuery("""
            select -id as id, parent, name, -id as prop
            from MultiPropGr where 0=0
            union all
            select id, -multiPropGr as parent, name, id as prop
            from MultiProp where 0=0
        """);
    }

    public Set<Object> propPeriodType(long prop) throws Exception {
        Store st = mdb.loadQuery("select periodType from PropPeriodType where prop=:p", Map.of("p", prop));

        return Collections.unmodifiableSet(st.getUniqueValues("periodType"));
    }

    public Store loadPropValEntityForSelect(long prop, long entityType) throws Exception {
        Store st = mdb.createStore("Prop.entity.select");

/*
        Store stEnt = mdb.loadQuery("select * from FD_EntityType where id=:id", Map.of("id", entityType));
        String tabl = stEnt.get(0).getString("tableName");

        if (stEnt.get(0).getString("code").equalsIgnoreCase("FactorVal")) {
            wheFV = "t.parent is not null";
        }
*/

        EntityConst.EntityInfo ei = EntityConst.getEntityInfo(entityType);
        String tabl = ei.getTableName();
        String wheFV = "";
        if (ei.getSign().equalsIgnoreCase("FactorVal")) {
            wheFV = " and t.parent is not null";
        }

        String sql = """
                select p.id, t.cod, t.name, t.fullName
                from PropVal p left join
                """ + tabl + " t on p.entityId=t.id where prop=:p";

        if (ei.getHasVer()) {
            sql = """
                    select p.id, t.cod, v.name, v.fullName
                    from PropVal p left join
                    """ + tabl + """ 
                     t on p.entityId=t.id left join
                    """ + tabl + "Ver v on t.id=v.ownerVer and v.lastVer=1 where prop=:p" + wheFV;
        }

        mdb.loadQuery(st, sql, Map.of("p", prop));
        return st;
    }


    public Store loadPropComplex(long prop) throws Exception {
        Store st = mdb.createStore("Prop.rec");
        mdb.loadQuery(st, """
                    WITH RECURSIVE r AS (
                        select p.id, p.parent from Prop p
                        where p.id=:p
                        union
                        select p.id, p.parent from Prop p
                            join r on p.parent=r.id
                    ),
                    p as (
                        SELECT * FROM r where 0=0--r.id<>p
                    )
                    select p.*, a.attribvaltype, ac.entitytype, m.meterStruct
                    from Prop p
                        left join attrib a on a.id=p.attrib
                        left join attribchar ac on ac.attrib=a.id
                        left join Meter m on p.meter=m.id
                    where p.id in (select id from p)
                """, Map.of("p", prop));

        //mdb.outTable(st);

        return st;
    }

    public StoreRecord newRecComplex(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore("Prop");
        StoreRecord r = st.add(rec);
        r.set("parent", rec.get("id"));
        r.set("id", null);
        r.set("cod", null);
        r.set("propType", null);
        r.set("name", null);
        r.set("fullName", null);
        r.set("allItem", null);
        r.set("ord", null);
        r.set("propTag", null);
        r.set("cmt", null);
        return r;
    }

    public String getParentName(long propGr, long parent) throws Exception {
        if (parent == 0) {
            return mdb.loadQuery("select name from PropGr where id=:id",
                    Map.of("id", propGr)).get(0).getString("name");
        } else {
            return mdb.loadQuery("select name from Prop where id=:id",
                    Map.of("id", parent)).get(0).getString("name");
        }
    }

    //
    public Store loadItemsComplexProp(long prop) throws Exception {
        Store st = mdb.createStore("Prop.rec");
        String sql = """
                    select p.*, m.kFromBase as kfc,
                        case when a.id is null then fp."text" else fa."text" end namePropType
                    from Prop p
                        left join fd_proptype fp on p.proptype=fp.id
                        left join attrib a on p.attrib=a.id
                        left join fd_attribvaltype fa on a.attribvaltype=fa.id
                        left join Measure m on p.measure=m.id
                    where p.parent=:prop
                """;
        return mdb.loadQuery(st, sql, Map.of("prop", prop));
    }

}
