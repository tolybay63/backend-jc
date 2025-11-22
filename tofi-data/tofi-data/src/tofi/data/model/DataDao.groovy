package tofi.data.model

import jandcode.commons.UtCnv
import jandcode.commons.UtDateTime
import jandcode.commons.UtString
import jandcode.commons.conf.Conf
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.IVariantMap
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.std.CfgService
import jandcode.core.std.DataDirService
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiMonitoringData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiUserData
import tofi.api.dta.model.utils.PeriodGenerator
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.ApiMetaData
import tofi.api.mdl.model.consts.FD_AttribValType_consts
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.api.mdl.model.consts.FD_MultiValEntityType_consts
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

import java.math.RoundingMode

class DataDao extends BaseMdbUtils {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }
    ApinatorApi apiMetaData() {
        return app.bean(ApinatorService).getApi("meta")
    }
    ApinatorApi apiUserData() {
        return app.bean(ApinatorService).getApi("userdata")
    }
    ApinatorApi apiNSIData() {
        return app.bean(ApinatorService).getApi("nsidata")
    }
    ApinatorApi apiMonitoringData() {
        return app.bean(ApinatorService).getApi("monitoringdata")
    }


    @DaoMethod
    Map<String, Object>
    getCurUserInfo() {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        AuthUser au = authSvc.getCurrentUser()
        if (au == null) {
            throw new XError("notLogined")
        }
        return au.getAttrs()
    }

    @DaoMethod
    Store loadDict(String dictName) throws Exception{
        return apiMeta().get(ApiMeta).loadDictAsStore(dictName)
    }

    void is_exist_owner_as_data(long owner, int isObj, String modelMeta) {

        Map<Long, Long> mapPV
        if (isObj == 1)
            mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("cls", false)
        else
            mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("relcls", false)

        List<String> lstApp = new ArrayList<>()
        long clsORrelcls
        if (isObj == 1) {
            clsORrelcls = apiUserData().get(ApiUserData).getClsOrRelCls(owner, isObj)
            if (mapPV.containsKey(clsORrelcls)) {
                boolean b = apiUserData().get(ApiUserData).is_exist_entity_as_dataOld(owner, "obj", mapPV.get(clsORrelcls))
                if (b) lstApp.add("userdata")
            }
            //
            clsORrelcls = apiNSIData().get(ApiNSIData).getClsOrRelCls(owner, isObj)
            if (mapPV.containsKey(clsORrelcls)) {
                boolean b = apiNSIData().get(ApiNSIData).is_exist_entity_as_dataOld(owner, "obj", mapPV.get(clsORrelcls))
                if (b) lstApp.add("nsidata")
            }
            //
            if (modelMeta == "fish") {
                clsORrelcls = apiMonitoringData().get(ApiMonitoringData).getClsOrRelCls(owner, isObj)
                if (mapPV.containsKey(clsORrelcls)) {
                    boolean b = apiMonitoringData().get(ApiMonitoringData).is_exist_entity_as_dataOld(owner, "obj", mapPV.get(clsORrelcls))
                    if (b) lstApp.add("monitoringdata")
                }
            }
        } else {
            clsORrelcls = apiUserData().get(ApiUserData).getClsOrRelCls(owner, isObj)
            if (mapPV.containsKey(clsORrelcls)) {
                boolean b = apiUserData().get(ApiUserData).is_exist_entity_as_dataOld(owner, "relobj", mapPV.get(clsORrelcls))
                if (b) lstApp.add("userdata")
            }
            //
            clsORrelcls = apiNSIData().get(ApiNSIData).getClsOrRelCls(owner, isObj)
            if (mapPV.containsKey(clsORrelcls)) {
                boolean b = apiNSIData().get(ApiNSIData).is_exist_entity_as_dataOld(owner, "relobj", mapPV.get(clsORrelcls))
                if (b) lstApp.add("nsidata")
            }
            //
            if (modelMeta == "fish") {
                clsORrelcls = apiMonitoringData().get(ApiMonitoringData).getClsOrRelCls(owner, isObj)
                if (mapPV.containsKey(clsORrelcls)) {
                    boolean b = apiMonitoringData().get(ApiMonitoringData).is_exist_entity_as_dataOld(owner, "relobj", mapPV.get(clsORrelcls))
                    if (b) lstApp.add("monitoringdata")
                }
            }
        }
        //...
        String msg = lstApp.join(", ")
        if (lstApp.size() > 0)
            throw new XError("UseInApp@" + msg)
    }

    void validateForDeleteOwner(long owner, int isObj, String modelMeta) {
        is_exist_owner_as_data(owner, isObj, modelMeta)
    }

    void deleteEntity(long id, int isObj, String model, String metaModel) {
        String tableName = isObj == 1 ? "Obj" : "RelObj"
        if (model == "userdata") {
            apiUserData().get(ApiUserData).deleteEntity(id, tableName)
        } else if (model == "nsidata") {
            apiNSIData().get(ApiNSIData).deleteEntity(id, tableName)
        } else if (metaModel == "fish") {
            if (model == "monitoringdata") {
                apiMonitoringData().get(ApiMonitoringData).deleteEntity(id, tableName)
            }
        } else {
            throw new XError("Unknown model [${model}]")
        }
    }

    @DaoMethod
    void deleteOwnerWithProperties(long id, int isObj, String model, String metaModel) {
        //
        validateForDeleteOwner(id, isObj, metaModel)
        //
        String sql1 = """
            delete from DataPropVal
            where dataProp in (select id from DataProp where isobj=${isObj} and objorrelobj=${id});
            delete from DataProp where id in (
                select id from dataprop
                except
                select dataProp as id from DataPropVal
            );
        """
        String sql2 = """
            delete from RelObjMember
            where relobj=${id}
        """

        execSql(sql1, model)
        if (isObj == 1) {
            deleteEntity(id, isObj, model, metaModel)
        } else {
            execSql(sql2, model)
            deleteEntity(id, isObj, model, metaModel)
        }
    }

    @DaoMethod
    Store loadRecObjOrRelObj(long own, boolean isObj, String model, String metamodel) {
        String domain = "Obj.full"
        String sql = """
            select *, v.objParent as parent 
            from Obj o, ObjVer v where o.id=v.ownerver and v.lastver=1 and o.id=${own}
        """
        if (!isObj) {
            domain = "RelObj.full"
            sql = """
                select * 
                from RelObj o, RelObjVer v where o.id=v.ownerver and v.lastver=1 and o.id=${own}
            """
        }
        return loadSql(sql, domain, model, metamodel)
    }

    @DaoMethod
    Map<String, Object> loadDataBase() {
        CfgService cfgSvc = getApp().bean(CfgService.class)
        String idmodel = cfgSvc.getConf().getString("dbsource/meta/id")
        Store st = apiMetaData().get(ApiMetaData).loadDataBase()
        return [store: st, metamodel: idmodel]
    }

    @DaoMethod
    Store loadOwnersParent(Map<String, Object> params) {
        return apiMetaData().get(ApiMetaData).loadOwnersParent(params)
    }

    @DaoMethod
    Store loadOwners(boolean isMultiProp, String node, long cls, long relCls, boolean isObj,
                     String model, String metamodel, long idOwn) {
        long id = UtCnv.toLong(node.split("_")[1])
        long typORrel = id
        String sql = ""
        if (!isObj) {   //RelObj
            long relTyp = loadSqlMeta("""
                        select relTyp from RelCls where id=${relCls}
                    """, "").get(0).getLong("relTyp")

            String whe = "r.relCls=${id}"
            if (idOwn > 0) {
                whe = "r.id=${idOwn}"
            }
            sql = """
                select 'ro_'||r.id as id, 'r_'||r.relCls as parent, r.cod, v.name, v.fullName, false as isObj,
                    true as loaded, r.id as node, r.relcls as typORrel, r.relCls, ${relTyp} as relTyp
                from RelObj r, RelObjVer v 
                where r.id=v.ownerVer and v.lastVer=1 and ${whe}
                order by r.id
            """
        } else {    //Obj
            String whe = ""
            if (idOwn > 0) {
                whe = "r.id=${idOwn}"
                typORrel = apiMeta().get(ApiMeta).getFKFromTable("Cls", cls, "typ")
            } else {
                Store stCls = apiMeta().get(ApiMeta).getClsFromTypCharGr(id, isMultiProp)
                whe = stCls.getUniqueValues("cls").join(",")
                whe = " r.cls in (0${whe}) and v.objParent is null"
                if (node.startsWith("o_")) {
                    whe = " v.objParent=${id}"
                    typORrel = apiMeta().get(ApiMeta).getFKFromTable("Cls", cls, "typ")
                }
            }

            sql = """
                select 'o_'||r.id as id,
                    case when v.objParent is null then 't_'||${id} else 'o_'||v.objParent end as parent,
                    r.id as node, ${typORrel} as typORrel, r.cod, v.name, v.fullName, true as isObj, r.cls, false as loaded, 0 as relTyp
                from Obj r, ObjVer v where r.id=v.ownerVer and v.lastVer=1 and ${whe}
                order by r.cls
            """
        }
        Store stRes = loadSql(sql, "DomainTreeOwns", model, metamodel)
        //mdb.outTable(stRes)
        return stRes
    }

    @DaoMethod
    Store loadTypCharGrProp(long tr, boolean isObj, boolean isFlat) {
        return apiMetaData().get(ApiMetaData).loadTypCharGrProp(tr, isObj, isFlat)
    }

    @DaoMethod
    Store loadCharGrMultiProp(long tr, boolean isObj) {
        return apiMetaData().get(ApiMetaData).loadCharGrMultiProp(tr, isObj)
    }

    @DaoMethod
    Map<String, Object> propsOfMultiProp(long dmp, long dmpi) {
        String sql = """
            select mp.id as multiprop, mp.cod, mpd.id as multipropdim, dmpi.id as dimMultiPropItem,
                multiEntityType, mp.isuniq, mp.isdependvalueonperiod as dependPeriod, coalesce(statusfactor, 0) as status, 
                coalesce(providertyp, 0) as provider, mpd.dimnumber, mpd.isfilled, fillmore, dmp.counthasvalue 
            from DimMultiPropItem dmpi
                left join MultiPropDim mpd on mpd.dimmultiprop=dmpi.dimmultiprop
                left join MultiProp mp on mp.id=mpd.multiprop 
                left join DimMultiProp dmp on dmp.id=dmpi.dimmultiprop 
            where dmpi.dimmultiprop=${dmp} and dmpi.id=${dmpi}
        """
        Store st = apiMetaData().get(ApiMetaData).loadSql(sql, "PropsOfMultiProp")
        long multiProp = st.get(0).getLong("multiProp")
        Map<String, Object> mapRez = new HashMap<>()
        if (st.size() > 0)
            mapRez.putAll(st.get(0).getValues())

        if (st.get(0).getLong("multiEntityType") == FD_MultiValEntityType_consts.meter) {
            String sqlTmp = """
                select m.id as measure, m.name as measureName, m.kFromBase as kfc, d.minVal, d.maxVal, d.digit
                from DimMultiPropItemMeter d, Measure m
                where d.measure=m.id and d.dimMultiPropItem=${st.get(0).getLong("dimMultiPropItem")}
            """
            Store stTmp = apiMetaData().get(ApiMetaData).loadSql(sqlTmp, "Meter.info")
            //mdb.outTable(stTmp)
            if (stTmp.size() > 0)
                mapRez.putAll(stTmp.get(0).getValues())
        }

        if (st.get(0).getLong("status") > 0) {
            Store stTmp = apiMetaData().get(ApiMetaData).loadSql("select id from MultiPropStatus where multiProp=${multiProp}", "")
            if (stTmp.size() == 0) {
                throw new XError("notStatus: {0}", st.get(0).getString("cod"));
            }
        }

        if (st.get(0).getLong("provider") > 0) {
            Store stTmp = apiMetaData().get(ApiMetaData).loadSql("select id from MultiPropProvider where multiProp=${multiProp}", "")
            if (stTmp.size() == 0) {
                throw new XError("notProvider: {0}", st.get(0).getString("cod"));
            }
        }


        return mapRez


    }

    @DaoMethod
    Map<String, Object> propsOfProp(long prop) {
        String sql = """
            with ptp as (
                select prop, periodType from PropPeriodType where prop=${prop} limit 1
            ),
            attr as (
                select p.id, fa.id as attribValType, ac.maskreg, ac.format,ac.entitytype, ac.periodtype, ac.fileext
                from prop p
                    left join attrib a on p.attrib=a.id
                    left join fd_attribvaltype fa on a.attribvaltype=fa.id
                    left join attribchar ac on a.id=ac.attrib
                where p.attrib is not null and p.id=${prop}
                order by ac.id limit 1
            )
            select
                p.cod, p.proptype, p.visualformat, attr.attribValType, attr.maskreg,attr.format,attr.entitytype, attr.fileext,
                p.isdependvalueonperiod as dependperiod, p.isdependnameonperiod as depenperiodname, p.isuniq,
                p.statusfactor, p.statusfactor is not null as status, p.providertyp, p.providertyp is not null as provider,
                p.measure, m.name as measurename, m.kfrombase as kfc, p.minval, p.maxval,
                ptp.periodtype, attr.periodtype as periodtypeattr
            from prop p
                left join fd_proptype pt on p.proptype=pt.id
                left join measure m on m.id=p.measure
                left join ptp on ptp.prop=p.id
                left join attr on attr.id=p.id
            where p.id=${prop}
        """
        Store st = apiMetaData().get(ApiMetaData).loadSql(sql, "PropsOfProp")
        if (st.get(0).getLong("statusFactor") > 0) {
            Store stTmp = apiMetaData().get(ApiMetaData).loadSql("select id from PropStatus where prop=${prop}", "")
            if (stTmp.size() == 0) {
                throw new XError("notStatus: {0}", st.get(0).getString("cod"));
            }
        }

        if (st.get(0).getLong("providerTyp") > 0) {
            Store stTmp = apiMetaData().get(ApiMetaData).loadSql("select id from PropProvider where prop=${prop}", "")
            if (stTmp.size() == 0) {
                throw new XError("notProvider: {0}", st.get(0).getString("cod"));
            }
        }
        Map<String, Object> mapRez = new HashMap<>()
        mapRez.put("props", st)
        return mapRez
    }

    @DaoMethod
    Store loadProvider(long prop, String model, String metamodel) {
        String sql = """
            select p.cls as id, p.obj, v.name, p.isdefault, d.modelname
            from PropProvider p
                left join Cls c on c.id=p.cls
                left join ClsVer v on c.id=v.ownerVer and v.lastVer=1
                left join Database d on c.database=d.id
            where prop=${prop} and d.modelname='${model}'
        """
        Store st = apiMetaData().get(ApiMetaData).loadSql(sql, "")
        Set<Object> idsObj = st.getUniqueValues("obj")
        String whe = "(0" + idsObj.join(",") + ")"
        //if (whe.isEmpty()) whe = 0
        //if (idsObj.size() > 0) {
        Store stObj = loadSql("""
                select o.id, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastver=1 and
                    o.id in ${whe}
            """, "", model, metamodel)

/*            StoreIndex stInd = stObj.getIndex("id")
            for(StoreRecord r in st) {
                StoreRecord rec = stInd.get(r.getLong("obj"))
                r.set("id", rec.getLong("id"))
                r.set("name", rec.getLong("name"))
            }*/
        //}
        return stObj
    }

    @DaoMethod
    Store loadStatus(long prop) throws Exception {
        String sql = """
            select f.id, f.name, p.isdefault, f.cod
            from PropStatus p
                left join Factor f on f.parent is not null and p.factorval=f.id
            where prop=${prop}
            order by f.ord
        """
        return apiMetaData().get(ApiMetaData).loadSql(sql, "")
    }

    Map<String, Object> loadPropValFacMeaStatusProvider(long prop, long propType, String model, String metamodel) {
        String sql = """
            select 
            case
                when p.factorval is not null then f.id
                when p.measure is not null then m.id
            end as id,

            case
                when p.factorval is not null then f.cod
                when p.measure is not null then m.cod
            end as cod,
            case
                when p.factorval is not null then f.name
                when p.measure is not null then m.name
            end as name,
            case
                when p.factorval is not null then f.fullName
                when p.measure is not null then m.fullName
            end as fullName, p.cls, p.relcls
            from PropVal p
                left join Factor f on p.factorval=f.id
                left join Measure m on p.measure =m.id
            where p.prop=${prop}
        """
        //
        Map<String, Object> res = new HashMap<>()
        if (propType == FD_PropType_consts.factor || propType == FD_PropType_consts.measure) {
            Store st = apiMetaData().get(ApiMetaData).loadSql(sql, "IdCodNameFullName.ent")
            res.put("store", st)
        }

        Store tmp = apiMetaData().get(ApiMetaData).loadSql(
                "select statusFactor, providerTyp from Prop where id=${prop}", "")

        if (tmp.get(0).getLong("statusFactor") > 0) {
            res.put("status", loadStatus(prop))
        }
        if (tmp.get(0).getLong("providerTyp") > 0) {
            res.put("provider", loadProvider(prop, model, metamodel))
        }
        return res
    }

    Map<String, Object> loadPropValObjORrel(long prop, long propType, String model, String metamodel) {
        String sql = """
            select cls, relcls from PropVal where prop=${prop}
        """
        Store st = apiMetaData().get(ApiMetaData).loadSql(sql, "")
        //
        Map<String, Object> res = new HashMap<>()

        String table = "Obj"
        String fls = "cls"
        Set<Object> ids = st.getUniqueValues("cls")
        if (propType == FD_PropType_consts.reltyp) {
            table = "RelObj"
            fls = "relcls"
            ids = st.getUniqueValues("relcls")
        }
        sql = """
                select o.id, o.cod, v.name, v.fullName
                from ${table} o, ${table}Ver v
                where o.id=v.ownerVer and v.lastVer=1 and ${fls} in (0${ids.join(",")})
            """
        st = loadSql(sql, "IdCodNameFullName", model, metamodel)
        res.put("store", st)
        return res
    }

    @DaoMethod
    Map<String, Object> loadDataRef(Map<String, Object> params, String domain) {
        IVariantMap imap = new VariantMap(params)
        String sql = """
            select d.id as dataProp, d.isObj, d.prop, d.status, d.provider, d.periodType, null as period,
                v.propval, v.obj, v.relobj, null as factorVal, null as measure,
                v.id, v.dataProp,
                v.dbeg, v.dend, v.parent, v.inputtype, v.ord, v.authuser, v.cmt, v."timestamp"
            from DataProp d inner join DataPropVal v on d.id=v.dataProp
            where d.objorrelobj=${imap.getLong("owner")} and d.prop=${imap.getLong("prop")} and d.isObj=${imap.getInt("isObj")}
        """
        Store st = loadSql(sql, domain, imap.getString("model"), imap.getString("metamodel"))
        //
        boolean isRefTR = imap.getLong("propType") == 5 || imap.getLong("propType") == 6
        String modelInt = imap.getString("model")

        Map<String, Object> mapPV_F_M_S_P = loadPropValFacMeaStatusProvider(imap.getLong("prop"), imap.getLong("propType"),
                imap.getString("model"), imap.getString("metamodel"))
        //
        Map<Long, Long> mapIdFV = apiMetaData().get(ApiMetaData).mapIdEntityFromPV("factorVal", imap.getLong("prop"))
        Map<Long, Long> mapIdMea = apiMetaData().get(ApiMetaData).mapIdEntityFromPV("measure", imap.getLong("prop"))
        for (StoreRecord r in st) {
            if (imap.getLong("propType") == 1)
                r.set("factorVal", mapIdFV.get(r.getLong("propVal")))
            if (imap.getLong("propType") == 7)
                r.set("measure", mapIdMea.get(r.getLong("propVal")))
        }
        //
        //mdb.outTable(st)
        //

        for (StoreRecord r in st) {
            if (isRefTR) {
                modelInt = loadSqlMeta("""
                        select case when c.id is not null then db.modelname when rc.id is not null then db2.modelname end as modelname
                        from PropVal pv
                        left join Cls c on c.id=pv.cls
                        left join database db on db.id=c.database 
                        left join RelCls rc on rc.id=pv.relcls
                        left join database db2 on db2.id=rc.database
                        where pv.id=${r.getLong("propVal")}
                    """, "").get(0).getString("modelname")
            }
        }
        Map<String, Object> mapPV_O_R = new HashMap<>()
        if (isRefTR) {
            mapPV_O_R = loadPropValObjORrel(imap.getLong("prop"), imap.getLong("propType"), modelInt, imap.getString("metamodel"))
        }


        PeriodGenerator pg = new PeriodGenerator()
        st.forEach(r -> {
            if (r.getString("dbeg").startsWith("0000"))
                r.set("dbeg", "1800-01-01")
            if (r.getString("dend").startsWith("0000"))
                r.set("dend", "3333-12-31")
            if (r.getLong("periodType") > 0) {
                String nm = pg.getPeriodName(r.getDate("dbeg"), r.getDate("dend"),
                        r.getLong("periodType"), 4)
                r.set("period", nm)
            } else {
                String d1, d2;
                if (r.getString("dbeg") == "1800-01-01")
                    d1 = "..."
                else
                    d1 = r.getDate("dbeg").toString(UtDateTime.createFormatter("dd.MM.yyyy"));
                if (r.getString("dend") == "3333-12-31")
                    d2 = "..."
                else
                    d2 = r.getDate("dend").toString(UtDateTime.createFormatter("dd.MM.yyyy"))

                r.set("period", d1 + " - " + d2)
            }
        })

        Map<String, Object> res = new HashMap<>()
        res.put("store", st)
        if (mapPV_O_R.size() > 0)
            res.put("storePV_TR", mapPV_O_R.get("store"))
        if (mapPV_F_M_S_P.size() > 0) {
            if (mapPV_F_M_S_P.keySet().contains("store"))
                res.put("storePV_FacORMea", mapPV_F_M_S_P.get("store"))
            if (mapPV_F_M_S_P.keySet().contains("status"))
                res.put("storeStatus", mapPV_F_M_S_P.get("status"))
            if (mapPV_F_M_S_P.keySet().contains("provider"))
                res.put("storeProvider", mapPV_F_M_S_P.get("provider"))
        }
        return res
    }

    private Map<String, Object> loadDataMulti(IVariantMap params) {
        int isObj = params.getBoolean("isObj") ? 1 : 0
        params.put("isObj", isObj)
        params.put("objOrRelObj", params.getLong("owner"))
        String domain

        if (params.getLong("entityType") == FD_MultiValEntityType_consts.meter) {
            domain = "DataMultiPropCell.meter.list"
        } else
            throw new XError("Unknown MultiPropType")

        String sql = """
            select c.id, d.id as dataMultiProp, d.isobj, d.objorrelobj, d.multiprop, coalesce(status, 0) as status,  coalesce(provider, 0) as provider, coalesce(periodtype, 0) as periodtype,
                c.numberval, c.measure, null as measureName, null as kfc, null as digit, null as minVal, null as maxVal, c.dbeg, c.dend, c.inputtype, c.authuser, c.cmt 
            from DataMultiProp d, DataMultiPropCell c, DataMultiPropCellCoord coo
            where d.id=c.datamultiprop and c.id=coo.datamultipropcell 
                and d.isobj=:isObj and d.objorrelobj=:objOrRelObj and d.multiprop=:multiProp and coo.multipropdim=:multiPropDim and coo.dimmultipropitem=:dimMultiPropItem 
        """
        Store st = loadSqlWithParams(sql, domain, params, params.getString("model"), params.getString("metamodel"))

        String sqlMea = """
            select d.measure, d.minVal, d.maxVal, d.digit, m.kFromBase as kfc, m.name as measureName
            from DimMultiPropItemMeter d, Measure m
            where d.measure=m.id and d.dimMultiPropItem=${params.getLong("dimMultiPropItem")}
        """
        Store stMea = apiMetaData().get(ApiMetaData).loadSql(sqlMea, "Meter.info")
        StoreIndex indMea = stMea.getIndex("measure")

        PeriodGenerator pg = new PeriodGenerator()
        for (StoreRecord r : st) {
            StoreRecord rec = indMea.get(r.getLong("measure"))
            if (rec != null) {
                r.set("measureName", rec.getString("measureName"))
                double k = rec.getDouble("kfc") == 0 ? 1.0 : rec.getDouble("kfc")
                r.set("kfc", k)
                if (rec.isValueNull("minVal"))
                    r.set("minVal", rec.getDouble("minVal"))
                if (rec.isValueNull("maxVal"))
                    r.set("maxVal", rec.getDouble("maxVal"))
                if (rec.isValueNull("digit"))
                    r.set("digit", rec.getDouble("digit"))
            }

            if (r.getLong("periodType") > 0) {
                String nm = pg.getPeriodName(r.getDate("dbeg"), r.getDate("dend"),
                        r.getLong("periodType"), 4);
                r.set("period", nm);
            } else {
                String d1, d2;
                if (r.getString("dbeg") == "1800-01-01")
                    d1 = "...";
                else
                    d1 = r.getDate("dbeg").toString(UtDateTime.createFormatter("dd.MM.yyyy"));
                if (r.getString("dend") == "3333-12-31")
                    d2 = "...";
                else
                    d2 = r.getDate("dend").toString(UtDateTime.createFormatter("dd.MM.yyyy"));

                r.set("period", d1 + " - " + d2);
            }
        }


        Map<String, Object> res = new HashMap<>()
        res.put("store", st)
        return res
    }

    @DaoMethod
    Map<String, Object> loadData(Map<String, Object> params) {
        IVariantMap imap = new VariantMap(params)
        //todo Filter to dt!

        if (imap.getBoolean("isMulti"))
            return loadDataMulti(imap)

        long propType = imap.getLong("propType")
        boolean isComplexItem = imap.getInt("isComplexItem") == 1
        int isObj = imap.getBoolean("isObj") ? 1 : 0
        imap.put("isObj", isObj)
        String domain

        if (isComplexItem) {
            return loadDataComplexItems(imap)
        } else if (propType == FD_PropType_consts.attr) {
            if (imap.getLong("attribValType") == FD_AttribValType_consts.file)
                domain = "DataPropVal.attrib.file.list"
            else
                domain = "DataPropVal.attrib.list"
        } else if (propType == FD_PropType_consts.meter || propType == FD_PropType_consts.rate)
            domain = "DataPropVal.meter.list"
        else if (propType == FD_PropType_consts.factor || propType == FD_PropType_consts.typ
                || propType == FD_PropType_consts.reltyp || propType == FD_PropType_consts.measure) {
            domain = "DataPropVal.ref.list"
            return loadDataRef(imap, domain)
        } else if (propType == FD_PropType_consts.complex)
            domain = "DataPropVal.complex.list"
        else
            throw new XError("Unknown proptype")
        //
        String sql = """
            select d.id as dataProp, d.isObj, d.prop, d.status, d.provider, d.periodType, null as period,
                null as fileName, null as digit, null as kfrombase,
                v.id, v.dataProp, v.numberval, v.strVal, v.multistrval, v.datetimeval, v.propval, v.fileval,
                v.dbeg, v.dend, v.parent, v.inputtype, v.ord, v.authuser, v.cmt, v."timestamp"
            from DataProp d inner join DataPropVal v on d.id=v.dataProp
            where d.objorrelobj=${imap.getLong("owner")} and d.prop=${imap.getLong("prop")} and d.isObj=${imap.getInt("isObj")}
        """
        Store st = loadSql(sql, domain, imap.getString("model"), imap.getString("metamodel"))
        //
        String sqlProp = """
            select p.id, p.digit, m.kfrombase from Prop p
                left join Measure m on  m.id=p.measure
            where p.id=${imap.getLong("prop")}
        """
        Store stProp = apiMeta().get(ApiMeta).loadSql(sqlProp, "")
        StoreIndex indexProp = stProp.getIndex("id")
        StoreIndex indexFS = null
        if (imap.getLong("propType") == FD_PropType_consts.attr &&
                imap.getLong("attribValType") == FD_AttribValType_consts.file) {
            Set<Object> setFile = st.getUniqueValues("fileval")
            String whe = setFile.join(",")
            if (whe.isEmpty()) whe = 0
            Store stFS = apiMeta().get(ApiMeta).loadSql("""
                select id, originalfilename from DbFileStorage where id in (${whe}) 
            """, "")
            indexFS = stFS.getIndex("id")
        }

        for (StoreRecord r in st) {
            if (propType == FD_PropType_consts.meter ||
                    propType == FD_PropType_consts.rate) {
                StoreRecord rec = indexProp.get(r.getLong("prop"))
                r.set("numberval", r.getDouble("numberval") * rec.getDouble("kfrombase"))
                if (!rec.isValueNull("digit"))
                    r.set("digit", rec.getLong("digit"))
            }
            if (imap.getLong("attribValType") == FD_AttribValType_consts.file) {
                StoreRecord rec2 = indexFS.get(r.getLong("fileval"))
                if (rec2 != null)
                    r.set("fileName", rec2.getString("originalfilename"))
            }
        }
        //
        PeriodGenerator pg = new PeriodGenerator()
        st.forEach(r -> {
            if (propType == FD_PropType_consts.meter || propType == FD_PropType_consts.rate) {
                if (!r.isValueNull("digit")) {
                    double v = round(r.getDouble("numberVal"), r.getInt("digit"))
                    r.set("numberVal", v)
                }
            }
            if (r.getString("dbeg").startsWith("0000"))
                r.set("dbeg", "1800-01-01");
            if (r.getString("dend").startsWith("0000"))
                r.set("dend", "3333-12-31");
            if (r.getLong("periodType") > 0) {
                String nm = pg.getPeriodName(r.getDate("dbeg"), r.getDate("dend"),
                        r.getLong("periodType"), 4);
                r.set("period", nm);
            } else {
                String d1, d2;
                if (r.getString("dbeg") == "1800-01-01")
                    d1 = "...";
                else
                    d1 = r.getDate("dbeg").toString(UtDateTime.createFormatter("dd.MM.yyyy"));
                if (r.getString("dend") == "3333-12-31")
                    d2 = "...";
                else
                    d2 = r.getDate("dend").toString(UtDateTime.createFormatter("dd.MM.yyyy"));

                r.set("period", d1 + " - " + d2);
            }
        })

        Map<String, Object> res = new HashMap<>()
        //
        if (imap.getLong("attribValType") == FD_AttribValType_consts.entity) {
            Store stPV = apiMetaData().get(ApiMetaData).loadPropValEntityForSelect(imap.getLong("prop"), imap.getLong("entityType"))
            Map<Long, Map<String, Object>> map = new HashMap<>()
            for (StoreRecord r : stPV) {
                map.put(r.getLong("id"), r.getValues())
            }
            res.put("mapPVent", map)
        }
        res.put("store", st)
        ////
        if (imap.getLong("attribValType") == FD_AttribValType_consts.file) {
            String path = ""
            try {
                path = mdb.getApp().bean(DataDirService.class).getPath("dbfilestorage")
                path = path + File.separator + imap.getString("model")
            } catch (Exception e) {
                path = ""
                e.printStackTrace()
            }

            String bucketName = ""
            try {
                Conf conf2 = mdb.getApp().getConf().getConf("datadir/minio")
                bucketName = conf2.getString("bucketName")
            } catch (Exception e) {
                bucketName = ""
                //e.printStackTrace()
            }

            if (!path.isEmpty() && bucketName.isEmpty()) {
                res.put("isMinio", false)
            } else if (path.isEmpty() && !bucketName.isEmpty()) {
                res.put("isMinio", true)
            } else {
                throw new XError("FileStorage не настроен!")
            }
        }
        ////
        return res
    }

    private Map<String, Object> loadDataComplexItems(IVariantMap params) {
        Map<String, Object> res = new HashMap<>();
        long idDataComplex = params.getLong("idDataComplex")

        String domain = "DataPropVal.complex.item.list"
        String sql = """
            select 
                null as propName, null as proptype, null as isuniq, null as attribvaltype, null as entitytype, 
                null as digit, null as kfrombase, null as val,
                d.isobj, d.prop, d.provider, d.status, d.periodtype,
                v.numberval, v.strval, v.dateTimeVal, v.propVal, v.obj, v.relobj,                
                v.id, v.dataprop, v.dbeg, v.dend, v.inputtype, v.authuser, v.parent, v.ord, v.cmt
            from DataPropVal v
                inner join DataProp d on d.id=v.dataprop
            where v.parent=${idDataComplex}
            order by ord         
        """
        Store st = loadSql(sql, domain,
                params.getString("model"), params.getString("metamodel"))
        //
        //mdb.outTable(st)
        //
        String sqlProp = """
            select p.id, p.proptype, p.isuniq, p.name, p.digit, m.kfrombase, ac.entitytype, a.attribvaltype
            from Prop p
                left join Measure m on  m.id=p.measure
                left join Attrib a on a.id=p.attrib
                left join attribchar ac on ac.attrib=a.id  
            where p.parent=${params.getLong("prop")}
        """
        Store stProp = apiMeta().get(ApiMeta).loadSql(sqlProp, "")

        //mdb.outTable(stProp)

        StoreIndex indProp = stProp.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord rec = indProp.get(r.getLong("prop"))
            if (rec == null) continue
            if (rec.getLong("propType") == 2 || rec.getLong("propType") == 3) {
                Double v = r.isValueNull("numberVal") ? null : r.getDouble("numberVal") * rec.getDouble("kfrombase")
                r.set("kfrombase", rec.getLong("kfrombase"))
                if (!rec.isValueNull("digit"))
                    r.set("digit", rec.getLong("digit"))
                r.set("val", v)
            }
            if (rec.getLong("propType") == 4 && rec.getLong("attribvaltype") < 3) {
                String v = r.isValueNull("strVal") ? null : r.getString("strVal")
                r.set("val", v)
            }
            if (rec.getLong("propType") == 4 && rec.getLong("attribvaltype") > 2 && rec.getLong("attribvaltype") < 6) {
                String v = r.isValueNull("dateTimeVal") ? null : r.getString("dateTimeVal")
                r.set("val", v)
            }
            if (rec.getLong("propType") == 1 || (rec.getLong("propType") > 4 && rec.getLong("propType") < 8)) {
                r.set("val", r.getLong("propVal"))
            }
            if (rec.getLong("propType") == 4 && rec.getLong("attribvaltype") == 8) {
                r.set("val", r.getLong("propVal"))
            }
            r.set("proptype", rec.getLong("proptype"))
            r.set("attribvaltype", rec.getLong("attribvaltype"))
            r.set("isuniq", rec.getInt("isuniq"))
            r.set("propName", rec.getString("name"))
            r.set("entitytype", rec.getLong("entitytype"))
        }


        //
        IVariantMap mapTmp = new VariantMap()
        IVariantMap mapRef = new VariantMap()
        PeriodGenerator pg = new PeriodGenerator()
        st.forEach(r -> {
            if (r.getLong("attribValType") == FD_AttribValType_consts.entity) {
                mapTmp.put("isPV", 0L)
                mapTmp.put("prop", r.getLong("prop"))
                mapTmp.put("entityType", r.getLong("entityType"))
            }
            if (r.getLong("propType") == FD_PropType_consts.factor) {
                try {
                    setPropValRef(mapRef, r.getLong("prop"), "factorVal",
                            params.getString("model"), params.getString("metamodel"))
                } catch (Exception e) {
                    e.printStackTrace()
                }
            }
            if (r.getLong("propType") == FD_PropType_consts.typ) {
                try {
                    setPropValRef(mapRef, r.getLong("prop"), "obj",
                            params.getString("model"), params.getString("metamodel"))
                } catch (Exception e) {
                    e.printStackTrace()
                }
            }
            if (r.getLong("propType") == FD_PropType_consts.reltyp) {
                try {
                    setPropValRef(mapRef, r.getLong("prop"), "relobj",
                            params.getString("model"), params.getString("metamodel"))
                } catch (Exception e) {
                    e.printStackTrace()
                }
            }
            if (r.getLong("propType") == FD_PropType_consts.measure) {
                try {
                    setPropValRef(mapRef, r.getLong("prop"), "measure",
                            params.getString("model"), params.getString("metamodel"))
                } catch (Exception e) {
                    e.printStackTrace()
                }
            }
            if (r.getLong("propType") == FD_PropType_consts.meter || r.getLong("propType") == FD_PropType_consts.rate) {
                if (!r.isValueNull("digit")) {
                    if (!r.isValueNull("val")) {
                        double v = round(r.getDouble("val"), r.getInt("digit"))
                        r.set("val", UtString.toString(v))
                    }
                }
            }
            if (r.getString("dbeg").startsWith("0000"))
                r.set("dbeg", "1800-01-01")
            if (r.getString("dend").startsWith("0000"))
                r.set("dend", "3333-12-31")
            if (r.getLong("periodType") > 0) {
                String nm = pg.getPeriodName(r.getDate("dbeg"), r.getDate("dend"),
                        r.getLong("periodType"), 4)
                if (r.getString("dbeg") == "1800-01-01" && r.getString("dend") == "3333-12-31")
                    nm = "..."
                r.set("period", nm)
            } else {
                String d1, d2
                if (r.getString("dbeg") == "1800-01-01")
                    d1 = "..."
                else
                    d1 = r.getDate("dbeg").toString(UtDateTime.createFormatter("dd.MM.yyyy"))
                if (r.getString("dend") == "3333-12-31")
                    d2 = "..."
                else
                    d2 = r.getDate("dend").toString(UtDateTime.createFormatter("dd.MM.yyyy"))

                r.set("period", d1 + " - " + d2)
            }
        })

        if (mapTmp.containsKey("isPV")) {
            Store stPV = loadPropValEntityForSelect(mapTmp.getLong("prop"), mapTmp.getLong("entityType"))
            Map<Long, Map<String, Object>> map = new HashMap<>()
            for (StoreRecord r : stPV) {
                map.put(r.getLong("id"), r.getValues())
            }
            res.put("mapPVent", map)
            //mdb.outMap(map)
        }

        //mdb.outTable(st)
        //mdb.outMap(mapRef)

        res.put("mapPVref", mapRef)
        res.put("store", st)
        return res
    }

    private void setPropValRef(IVariantMap map, long prop, String fld, String model, String metamodel) throws Exception {

        String sql = "", px = "", domain = "IdCodNameFullName"
        boolean isMeta = true
        if (fld.equalsIgnoreCase("factorVal")) {
            px = "f_"
            sql = """
                select p.id, f.cod, f.name, f.fullName from PropVal p, Factor f
                where p.prop=${prop} and p.factorVal=f.id
            """
        } else if (fld.equalsIgnoreCase("measure")) {
            px = "m_"
            sql = """
                select p.id, m.cod, m.name, m.fullName from PropVal p, Measure m
                where p.prop=${prop} and p.measure=m.id
            """
        } else if (fld.equalsIgnoreCase("obj")) {
            isMeta = false
            px = "o_"
            Set<Object> idsCls = apiMeta().get(ApiMeta).loadSql("select cls from PropVal where prop=${prop}", "")
                    .getUniqueValues("cls")
            idsCls.add(0L)
            sql = """
                select o.id, o.cod, v.name, v.fullName from Obj o, ObjVer v
                where o.id=v.ownerver and v.lastVer=1 and o.cls in (${idsCls.join(",")})                    
            """
        } else if (fld.equalsIgnoreCase("relobj")) {
            isMeta = false
            px = "r_"
            Set<Object> idsRelCls = apiMeta().get(ApiMeta).loadSql("select relcls from PropVal where prop=${prop}", "")
                    .getUniqueValues("relcls")
            idsRelCls.add(0L)
            sql = """
                select o.id, o.cod, v.name, v.fullName from RelObj o, RelObjVer v
                where o.id=v.ownerver and v.lastVer=1 and o.relcls in (${idsRelCls.join(",")})                    
            """
        }

        Store st
        if (isMeta)
            st = apiMeta().get(ApiMeta).loadSql(sql, domain)
        else
            st = loadSql(sql, domain, model, metamodel)
        for (StoreRecord r : st) {
            map.put(px + r.getString("id"), r.getValues())
        }
    }

    @DaoMethod
    Set<Object> propPeriodType(long prop, boolean isMulti) {
        String sql = "select periodType from PropPeriodType where prop=${prop}"
        if (isMulti)
            sql = "select periodType from MultiPropPeriodType where multiprop=${prop}"

        Store st = apiMeta().get(ApiMeta).loadSql(sql, "")
        return st.getUniqueValues("periodType")
    }

    @DaoMethod
    Store loadPropValEntityForSelect(long prop, long entityType) {
        return apiMetaData().get(ApiMetaData).loadPropValEntityForSelect(prop, entityType)
    }

    @DaoMethod
    Map<String, Object> getPeriodInfo(XDate dt, long periodType) {
        return apiMeta().get(ApiMeta).getPeriodInfo(dt, periodType)
    }

    @DaoMethod
    void insertAttribValue(Map<String, Object> rec) {
        IVariantMap par = new VariantMap(rec)

        par.putIfAbsent("dbeg", "1800-01-01")
        par.putIfAbsent("dend", "3333-12-31")
        if (par.getLong("periodType") == 0) par.remove("periodType")
        //
        if (par.getBoolean("isUniq"))
            validateDataUniq(par)
        //
        if (Arrays.asList(FD_AttribValType_consts.str, FD_AttribValType_consts.mask)
                .contains(par.getLong("attribValType"))) {
            if (par.isValueNull("strVal")) {
                throw new XError("value is required")
            }
        } else if (Arrays.asList(FD_AttribValType_consts.dt, FD_AttribValType_consts.dttm, FD_AttribValType_consts.tm)
                .contains(par.getLong("attribValType"))) {
            if (par.isValueNull("dateTimeVal")) {
                throw new XError("value is required")
            }
        } else if (par.getLong("attribValType") == FD_AttribValType_consts.multistr) {
            if (par.isValueNull("multiStrVal")) {
                throw new XError("value is required")
            }
        } else if (par.getLong("attribValType") == FD_AttribValType_consts.entity) {
            if (par.isValueNull("propVal")) {
                throw new XError("value is required")
            }
        } else if (par.getLong("attribValType") == FD_AttribValType_consts.file) {
            if (par.isValueNull("fileVal")) {
                throw new XError("filevalue is required")
            }
        }
        //
        long id = insertRecToTable("DataProp", par, par.getString("model"), par.getString("metamodel"), true)
        //
        par.put("dataProp", id)
        long au = getUser()
        par.put("authUser", au)
        par.put("inputType", FD_InputType_consts.model)
        id = insertRecToTable("DataPropVal", par, par.getString("model"), par.getString("metamodel"), false)
    }

    @DaoMethod
    void deleteAttribValue(Map<String, Object> rec) {
        long idPropVal = UtCnv.toLong(rec.get("id"))
        String sql = """
            delete from DataPropVal where id=${idPropVal};
            with d as (
                select id from DataProp
                except
                select dataProp as id from DataPropVal
            )
            delete from DataProp where id in (select id from d);
        """
        execSql(sql, UtCnv.toString(rec.get("model")))
    }

    @DaoMethod
    void deleteFileValue(Map<String, Object> rec) {
        String path = ""
        try {
            path = mdb.getApp().bean(DataDirService.class).getPath("dbfilestorage")
        } catch (Exception e) {
            path = ""
            e.printStackTrace()
        }

        String bucketName = ""
        try {
            Conf conf2 = mdb.getApp().getConf().getConf("datadir/minio")
            bucketName = conf2.getString("bucketName")
        } catch (Exception e) {
            bucketName = ""
            e.printStackTrace()
        }

        if (path != "" && bucketName == "") {
            deleteFileValueFS(rec)
        } else if (path == "" && bucketName != "") {
            deleteFileValueMinio(rec)
        } else {
            throw new XError("FileStorage не настроен!");
        }
    }

    private void deleteFileValueFS(Map<String, Object> params) {
        long fileId = UtCnv.toLong(params.get("fileId"))
        long id = UtCnv.toLong(params.get("id"))

        try {
            DbFileStorageService dfsrv = apiMeta().get(ApiMeta).getDbFileStorageService()
            dfsrv.setModelName(UtCnv.toString(params.get("model")))
            dfsrv.removeFile(fileId)
        } finally {
            String sql = """
                delete from DataPropVal where id=${id};
                with d as (
                    select id from DataProp
                    except
                    select dataProp as id from DataPropVal
                )
                delete from DataProp where id in (select id from d);
            """
            execSql(sql, UtCnv.toString(params.get("model")))
        }
    }

    private static void deleteFileValueMinio(Map<String, Object> params) {
        throw new XError("MinIO не настроен!");
    }

    @DaoMethod
    void updateAttribValue(Map<String, Object> rec) throws Exception {
        IVariantMap par = new VariantMap(rec)
        par.putIfAbsent("dbeg", "1800-01-01")
        par.putIfAbsent("dend", "3333-12-31")
        //
        if (par.getBoolean("isUniq"))
            validateDataUniq(par)
        par.put("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME));

        if (Arrays.asList(FD_AttribValType_consts.str, FD_AttribValType_consts.mask)
                .contains(par.getLong("attribValType"))) {
            if (par.getString("strVal").isEmpty()) {
                throw new XError("value is required")
            }
        } else if (Arrays.asList(FD_AttribValType_consts.dt, FD_AttribValType_consts.dttm, FD_AttribValType_consts.tm)
                .contains(par.getLong("attribValType"))) {
            if (par.isValueNull("dateTimeVal")) {
                throw new XError("value is required")
            }
        } else if (par.getLong("attribValType") == FD_AttribValType_consts.multistr) {
            if (par.isValueNull("multiStrVal")) {
                throw new XError("value is required")
            }
        } else if (par.getLong("attribValType") == FD_AttribValType_consts.entity) {
            if (par.isValueNull("propVal")) {
                throw new XError("value is required")
            }
        } else if (par.getLong("attribValType") == FD_AttribValType_consts.file) {
            if (par.isValueNull("fileVal")) {
                throw new XError("value is required")
            }
        }
        //
        updateTable("DataPropVal", par, par.getString("model"), par.getString("metamodel"))
    }

    // **** Meter
    void insertMeterValueMultiProp(Map<String, Object> rec) throws Exception {
        IVariantMap par = new VariantMap(rec)
        validateDataUniq(par)
        //
        long id = insertRecToTable("DataMultiProp", par, par.getString("model"), par.getString("metamodel"), true)
        //
        par.put("dataMultiProp", id)
        long au = getUser()
        par.put("authUser", au)
        par.put("inputType", FD_InputType_consts.model)
        //
        double kfc = par.getDouble("kfc")
        if (kfc == 0) kfc = 1.0
        par.put("numberVal", par.getDouble("numberVal") / kfc)
        id = insertRecToTable("DataMultiPropCell", par, par.getString("model"), par.getString("metamodel"), false)
        Map<String, Object> params = new HashMap<>()
        params.put("dataMultiPropCell", id)
        params.put("multiPropDim", par.getLong("multiPropDim"))
        params.put("dimMultiPropItem", par.getLong("dimMultiPropItem"))
        insertRecToTable2("DataMultiPropCellCoord", params, par.getString("model"), par.getString("metamodel"), true)
    }

    void updateMeterValueMultiProp(Map<String, Object> rec) throws Exception {
        IVariantMap par = new VariantMap(rec)
        validateDataUniq(par)
        //
        double kfc = par.getDouble("kfc")
        if (kfc == 0) kfc = 1.0
        par.put("numberVal", par.getDouble("numberVal") / kfc)
        par.put("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
        updateTable("DataMultiPropCell", par, par.getString("model"), par.getString("metamodel"))
    }

    @DaoMethod
    void insertMeterValue(Map<String, Object> rec) throws Exception {
        IVariantMap par = new VariantMap(rec)
        par.putIfAbsent("dbeg", "1800-01-01")
        par.putIfAbsent("dend", "3333-12-31")
        if (par.getLong("periodType") == 0) par.remove("periodType")
        if (par.getLong("ststus") == 0) par.remove("ststus")
        if (par.getLong("provider") == 0) par.remove("provider")
        //
        if (par.getBoolean("isMulti")) {
            insertMeterValueMultiProp(par)
        } else {
            //
            validateDataUniq(par)
            //
            long id = insertRecToTable("DataProp", par, par.getString("model"), par.getString("metamodel"), true)
            //
            par.put("dataProp", id)
            long au = getUser()
            par.put("authUser", au)
            par.put("inputType", FD_InputType_consts.model)
            //
            double kfc = 1
            try {
                kfc = apiMeta().get(ApiMeta).getKfromBase(par.getLong("prop"))
            } catch (Exception e) {
                System.out.println("Error: Measure not found!")
                e.printStackTrace()
            }
            par.put("numberVal", par.getDouble("numberVal") / kfc)
            insertRecToTable("DataPropVal", par, par.getString("model"), par.getString("metamodel"), false)
        }
    }

    @DaoMethod
    void updateMeterValue(Map<String, Object> rec) {
        IVariantMap par = new VariantMap(rec)
        par.putIfAbsent("dbeg", "1800-01-01")
        par.putIfAbsent("dend", "3333-12-31")
        if (par.getLong("periodType") == 0) par.remove("periodType")
        if (par.getLong("ststus") == 0) par.remove("ststus")
        if (par.getLong("provider") == 0) par.remove("provider")
        //
        if (par.getBoolean("isMulti")) {
            updateMeterValueMultiProp(par)
        } else {
            validateDataUniq(par)
            double kfc = 1
            try {
                kfc = apiMeta().get(ApiMeta).getKfromBase(par.getLong("prop"))
            } catch (Exception e) {
                System.out.println("Error: kFromBase not found!")
                e.printStackTrace()
            }
            par.put("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
            par.put("numberVal", par.getDouble("numberVal") / kfc)
            //
            updateTable("DataPropVal", par, par.getString("model"), par.getString("metamodel"))
        }
    }

    @DaoMethod
    void deleteMeterValueMultiProp(Map<String, Object> rec) throws Exception {
        long idPropCell = UtCnv.toLong(rec.get("id"))
        long multiPropDim = UtCnv.toLong(rec.get("multiPropDim"))
        long dimMultiPropItem = UtCnv.toLong(rec.get("dimMultiPropItem"))

        String sql = """
            delete from DataMultiPropCellCoord 
            where dataMultiPropCell=${idPropCell} and multiPropDim=${multiPropDim} and dimMultiPropItem=${dimMultiPropItem};
            delete from DataMultiPropCell where id=${idPropCell};
            with d as (
                select id from DataMultiProp
                except
                select dataMultiProp as id from DataMultiPropCell
            )
            delete from DataMultiProp where id in (select id from d);
        """
        execSql(sql, UtCnv.toString(rec.get("model")))
    }

    @DaoMethod
    void deleteMeterValue(Map<String, Object> rec) throws Exception {
        long idPropVal = UtCnv.toLong(rec.get("id"))
        String sql = """
            delete from DataPropVal where id=${idPropVal};
            with d as (
                select id from DataProp
                except
                select dataProp as id from DataPropVal
            )
            delete from DataProp where id in (select id from d);
        """
        execSql(sql, UtCnv.toString(rec.get("model")))
    }

    @DaoMethod
    Map<String, Object> loadPropVal(long prop, long propType, String model, String metamodel) {
        String sql = """
            select 
            case
                when p.factorval is not null then f.id
                when p.measure is not null then m.id
            end as id,

            case
                when p.factorval is not null then f.cod
                when p.measure is not null then m.cod
            end as cod,
            case
                when p.factorval is not null then f.name
                when p.measure is not null then m.name
            end as name,
            case
                when p.factorval is not null then f.fullName
                when p.measure is not null then m.fullName
            end as fullName, p.cls, p.relcls
            from PropVal p
                left join Factor f on p.factorval=f.id
                left join Measure m on p.measure =m.id
            where p.prop=${prop}
        """
        Store st = apiMetaData().get(ApiMetaData).loadSql(sql, "IdCodNameFullName.ent")
        //
        Map<String, Object> res = new HashMap<>()
        if (propType == FD_PropType_consts.factor || propType == FD_PropType_consts.measure)
            res.put("store", st)
        if (propType == FD_PropType_consts.typ || propType == FD_PropType_consts.reltyp) {
            String table = "Obj"
            String fls = "cls"
            Set<Object> ids = st.getUniqueValues("cls")
            if (propType == FD_PropType_consts.reltyp) {
                table = "RelObj"
                fls = "relcls"
                ids = st.getUniqueValues("relcls")
            }
            sql = """
                select o.id, o.cod, v.name, v.fullName
                from ${table} o, ${table}Ver v
                where o.id=v.ownerVer and v.lastVer=1 and ${fls} in (0${ids.join(",")})
            """
            st = loadSql(sql, "IdCodNameFullName", model, metamodel)
            res.put("store", st)
        }

        Store tmp = apiMetaData().get(ApiMetaData).loadSql(
                "select statusFactor, providerTyp from Prop where id=${prop}", "")

        if (tmp.get(0).getLong("statusFactor") > 0) {
            res.put("status", loadStatus(prop))
        }
        if (tmp.get(0).getLong("providerTyp") > 0) {
            res.put("provider", loadProvider(prop, model, metamodel))
        }
        return res
    }

    @DaoMethod
    Map<String, Object> loadRefValueForUpd(Map<String, Object> params) throws Exception {
        //IVariantMap rec = new VariantMap(UtCnv.toMap(params.get("rec")))
        IVariantMap recParam = new VariantMap(UtCnv.toMap(params.get("reqParams")))
        long propType = recParam.getLong("propType")
        long prop = recParam.getLong("prop")
        long own = recParam.getLong("owner")
        String domain = "DataPropVal.ref.upd"
        String sql = """
            select 
                case
                    when p.factorval is not null then f.id
                    when p.measure is not null then m.id
                end as id,    
                case
                    when p.factorval is not null then f.cod
                    when p.measure is not null then m.cod
                end as cod,
                case
                    when p.factorval is not null then f.name
                    when p.measure is not null then m.name
                end as name,
                case
                    when p.factorval is not null then f.fullName
                    when p.measure is not null then m.fullName
                end as fullName, 
                p.id as propVal, p.cls, p.relcls, 
                '1800-01-01' as dbeg, '3333-12-31' as dend
            from PropVal p
                left join Factor f on p.factorval=f.id
                left join Measure m on p.measure =m.id
            where p.prop=${prop}
        """
        Store st = apiMetaData().get(ApiMetaData).loadSql(sql, domain)
        //
        if (propType == FD_PropType_consts.factor || propType == FD_PropType_consts.measure) {
            Set<Object> ids = st.getUniqueValues("propVal")
            String sqlData = """
                select 
                    v.id as dataPropVal, v.dataprop, v.propVal, d.periodType, v.dbeg, v.dend, d.status, d.provider, v.cmt,
                    case when v.id is null then false else true end as checked 
                from DataProp d
                    left join DataPropVal v on d.id=v.dataprop
                where d.prop=${prop} and d.objorrelobj=${own} and v.propVal in (${ids.join(",")})
            """
            Store stData = loadSql(sqlData, domain, recParam.getString("model"), recParam.getString("metamodel"))
            StoreIndex indData = stData.getIndex("propVal")
            for (StoreRecord r in st) {
                StoreRecord rr = indData.get(r.getLong("propVal"))
                if (rr != null) {
                    r.set("dataPropVal", rr.getLong("dataPropVal"))
                    r.set("dataprop", rr.getLong("dataprop"))
                    r.set("periodType", rr.getLong("periodType"))
                    r.set("status", rr.getLong("status"))
                    r.set("provider", rr.getLong("provider"))
                    r.set("dbeg", rr.getString("dbeg"))
                    r.set("dend", rr.getString("dend"))
                    r.set("cmt", rr.getString("cmt"))
                    r.set("checked", rr.getBoolean("checked"))
                }
            }

        } else if (propType == FD_PropType_consts.typ || propType == FD_PropType_consts.reltyp) {
            String table = "Obj"
            String fls = "cls"
            Map<Long, Long> mapCls = apiMeta().get(ApiMeta).mapEntityIdFromPV("cls", false)
            Map<Long, Long> mapRelCls = apiMeta().get(ApiMeta).mapEntityIdFromPV("relcls", false)
            Set<Object> ids = st.getUniqueValues("cls")
            if (propType == FD_PropType_consts.reltyp) {
                table = "RelObj"
                fls = "relcls"
                ids = st.getUniqueValues("relcls")
            }
            sql = """
                select o.id, o.cod, ov.name, ov.fullName, o.${fls},
                v.id as dataPropVal, v.dataprop, v.propVal, d.periodType, v.dbeg, v.dend, d.status, d.provider, v.cmt,
                case when v.id is null then false else true end as checked 
                from ${table} o
                    left join ${table}Ver ov on o.id=ov.ownerVer and ov.lastVer=1
                    left join DataProp d on d.prop=${prop} and d.objorrelobj=${own}
                    left join DataPropVal v on d.id=v.dataprop and v.obj=o.id
                where ${fls} in (0${ids.join(",")})
            """
            st = loadSql(sql, domain, recParam.getString("model"), recParam.getString("metamodel"))
            for (StoreRecord r in st) {
                if (r.getLong("cls") > 0)
                    r.set("propVal", mapCls.get(r.getLong("cls")))
                if (r.getLong("relcls") > 0)
                    r.set("propVal", mapRelCls.get(r.getLong("relcls")))
            }
        }
        Map<String, Object> mapRes = new HashMap<>()
        mapRes.put("store", st)
        //
        return mapRes;
    }

    @DaoMethod
    void saveRefValue(Map<String, Object> params) throws Exception {
        IVariantMap reqParams = new VariantMap(UtCnv.toMap(params.get("reqParams")))

        List st = (List) params.get("st")
        boolean isUniq = reqParams.getBoolean("isUniq")
        //Old values
        Store stOld = loadSqlWithParams("""
            select v.id from DataPropVal v, DataProp d
            where d.id=v.dataProp and d.objorrelobj=:owner and d.prop=:prop and d.isObj=:isObj
        """, "", reqParams, reqParams.getString("model"), reqParams.getString("metamodel"))

        Set<Object> setOldIds = stOld.getUniqueValues("id")
        //New values
        Set<Long> setNewIds = new HashSet<>()
        for (Object ob : st) {
            Map<String, Object> map = UtCnv.toMap(ob)
            if (UtCnv.toLong(map.get("dataPropVal")) > 0) {
                setNewIds.add(UtCnv.toLong(map.get("dataPropVal")))
            }
        }
        //Delete
        boolean bDel = false
        for (Object ob : setOldIds) {
            if (!setNewIds.contains(UtCnv.toLong(ob))) {
                deleteTable("DataPropVal", UtCnv.toLong(ob), reqParams.getString("model"),
                        reqParams.getString("metamodel"))
                bDel = true
            }
        }
        if (bDel)
            execSql("""
                delete from dataprop where id in (
                select id from dataprop
                except
                select dataprop as id from datapropval d
                )
            """, reqParams.getString("model"))
        //
        //Insert/Update
        for (Object ob : st) {
            Map<String, Object> map = UtCnv.toMap(ob)
            map.putIfAbsent("dbeg", "1800-01-01")
            map.putIfAbsent("dend", "3333-12-31")
            if (reqParams.getLong("periodType") > 0)
                map.put("periodType", reqParams.getLong("periodType"))
            if (reqParams.getLong("propType") == FD_PropType_consts.typ)
                map.put("obj", UtCnv.toLong(map.get("id")))
            else if (reqParams.getLong("propType") == FD_PropType_consts.reltyp)
                map.put("relobj", UtCnv.toLong(map.get("id")))
            else {
                map.remove("obj")
                map.remove("relobj")
            }

            if (!setOldIds.contains(UtCnv.toLong(map.get("dataPropVal")))) {
                //ins
                reqParams.put("mode", "ins")
                if (isUniq) {
                    reqParams.putAll(map)
                    validateDataUniq(reqParams)
                }
                long dataProp = UtCnv.toLong(map.get("dataProp"))
                if (dataProp == 0) {
                    dataProp = getDataProp(map, reqParams)
                }
                map.put("dataProp", dataProp)
                long au = getUser()
                if (reqParams.getLong("parent") > 0)
                    map.put("parent", reqParams.getLong("parent"))
                map.put("authUser", au)
                map.put("inputType", FD_InputType_consts.model);
                insertRecToTable("DataPropVal", map, reqParams.getString("model"), reqParams.getString("metamodel"), false)
            } else {
                //update
                reqParams.put("mode", "upd")
                if (isUniq) {
                    reqParams.putAll(map)
                    validateDataUniq(reqParams)
                }
                map.put("id", reqParams.get("dataProp"))
                map.put("isObj", reqParams.get("isObj"))
                map.put("objorrelobj", reqParams.get("owner"))
                map.put("prop", reqParams.get("prop"))
                if (UtCnv.toLong(map.get("status")) == 0)
                    map.remove("status")
                if (UtCnv.toLong(map.get("provider")) == 0)
                    map.remove("provider")
                if (reqParams.getBoolean("dependPeriod"))
                    map.put("periodType", reqParams.get("periodType"))
                else
                    map.remove("periodType")
                updateTable("DataProp", map, reqParams.getString("model"), reqParams.getString("metamodel"))
                map.put("id", map.get("dataPropVal"))
                map.put("ord", map.get("dataPropVal"))
                map.putIfAbsent("dbeg", "1800-01-01")
                map.putIfAbsent("dend", "3333-12-31")
                map.put("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
                if (reqParams.getLong("parent") > 0)
                    map.put("parent", reqParams.getLong("parent"))
                long au = getUser()
                map.put("authUser", au)
                map.put("inputType", FD_InputType_consts.model)
                updateTable("DataPropVal", map, reqParams.getString("model"), reqParams.getString("metamodel"))
            }
        }
    }

    @DaoMethod
    Store loadClsTree(Map<String, Object> params) {
        long cls = UtCnv.toLong(params.get("typOrCls"))
        params.put("typ", cls)
        if (UtCnv.toLong(params.get("level")) > 1) {
            Store stCls = apiMeta().get(ApiMeta).loadSql("select typ from Cls where id=${cls}", "")
            params.put("typ", stCls.get(0).getLong("typ"))
        }
        return apiMeta().get(ApiMeta).loadClsTree(params)
    }

    @DaoMethod
    Store createOwn(Map<String, Object> params) {

        long idOwn = createOwner(params, UtCnv.toString(params.get("model")), UtCnv.toString(params.get("metamodel")))
        String model = UtCnv.toString(params.get("model"))
        String metamodel = UtCnv.toString(params.get("metamodel"))
        boolean isMultiProp = UtCnv.toBoolean(params.get("isMultiProp"))

        if (UtCnv.toBoolean(params.get("isObj"))) {
            Store stTemp = loadSql("""
                select o.cls, v.objParent 
                from Obj o, ObjVer v 
                where o.id=${idOwn} and o.id=v.ownerVer and v.lastVer=1
            """, "", model, metamodel)

            long cls = stTemp.get(0).getLong("cls")
            long objParent = stTemp.get(0).getLong("objParent")
            String node = "t_${params.get("typ")}"
            if (objParent > 0)
                node = "t_${objParent}"
            return loadOwners(isMultiProp, node, cls, 0, true, model, metamodel, idOwn)
        } else {
            long relCls = UtCnv.toLong(params.get("relCls"))
            String node = "ro_${idOwn}"
            return loadOwners(isMultiProp, node, 0, relCls, false, model, metamodel, idOwn)
        }
    }

    @DaoMethod
    String getParentName(long id, String model, String metamodel) {
        Store st = loadSql("select name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${id}",
                "", model, metamodel)
        return st.get(0).getString("name")
    }

    @DaoMethod
    String getParentNameRO(long id) {
        Store st = loadSqlMeta("""
            select name from RelCls o, RelClsVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${id}
        """, "")
        return st.get(0).getString("name")
    }

    @DaoMethod
    Store loadObjTreeParent(long typ, long cls, long obj, String model, String metamodel) throws Exception {
        String whCls = apiMeta().get(ApiMeta).ListClsParents(typ, cls)

/*
        case when c.typ=${typ} then 0 else 1 end as isInh
* */

        String sql = """
            select * from
            (
                select o.id, o.cls, v.objParent as parent, v.name as label,
                    case when o.cls=${cls} then 1 else 0 end as isOwn,
                    0 as isInh
                from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1
            ) t
            where id <> ${obj} and cls in (${whCls})
        """
        Store st = loadSql(sql, "Obj.select", model, metamodel)

//mdb.outTable(st)

        Set<Object> ids = st.getUniqueValues("id")
        for (StoreRecord r in st) {
            if (!ids.contains(r.getLong("parent")))
                r.set("parent", null)
        }
        return st
    }

    @DaoMethod
    Store loadItemsComplexProp(long prop) {
        return apiMeta().get(ApiMeta).loadSqlWithParams("""
            select p.*, m.kFromBase as kfc,
                case when a.id is null then fp."text" else fa."text" end namePropType
            from Prop p
                left join fd_proptype fp on p.proptype=fp.id
                left join attrib a on p.attrib=a.id
                left join fd_attribvaltype fa on a.attribvaltype=fa.id
                left join Measure m on p.measure=m.id
            where p.parent=:prop
        """, "Prop.rec", [prop: prop])
    }

    @DaoMethod
    void insertComplexValue(Map<String, Object> params) {
        IVariantMap par = new VariantMap(params)
        if (par.isValueNull("strVal")) {
            throw new XError("value of complex property is required");
        }
        par.putIfAbsent("dbeg", "1800-01-01")
        par.putIfAbsent("dend", "3333-12-31")
        //
        if (par.getBoolean("isUniq"))
            validateDataUniq(par)
        //
        long idDP = insertRecToTable("DataProp", par, par.getString("model"), par.getString("metamodel"), true)
        //
        par.put("dataProp", idDP)
        long au = getUser()
        par.put("authUser", au)
        par.put("inputType", FD_InputType_consts.model)
        //
        long idVal = insertRecToTable("DataPropVal", par, par.getString("model"), par.getString("metamodel"), false)
        //
        par.put("parent", idVal)
        Store stItems = apiMeta().get(ApiMeta).loadSqlWithParams("""
            select id from Prop where parent=:id order by ord
        """, "", [id: par.getLong("prop")])

        par.setValue("strVal", null);
        for (StoreRecord rr : stItems) {
            //recDP = mdb.createStoreRecord("DataProp", par);
            par.put("prop", rr.getLong("id"))
            idDP = insertRecToTable("DataProp", par, par.getString("model"), par.getString("metamodel"), true)
            par.put("dataProp", idDP)
            par.put("cmt", null)
            au = getUser()
            par.put("authUser", au)
            par.put("inputType", FD_InputType_consts.model)
            insertRecToTable("DataPropVal", par, par.getString("model"), par.getString("metamodel"), false)
        }
    }

    @DaoMethod
    void deleteComplexValue(long idDataPropVal, String model) throws Exception {
        execSql("""
            delete from DataPropVal where parent=${idDataPropVal};
            delete from DataPropVal where id=${idDataPropVal};
            with d as (
                select id from DataProp
                except
                select dataProp as id from DataPropVal
            )
            delete from DataProp where id in (select id from d);
        """, model)
    }

    @DaoMethod
    void updateComplexValue(Map<String, Object> rec) throws Exception {
        IVariantMap par = new VariantMap(rec)
        if (par.getString("strVal").isEmpty()) {
            throw new XError("value of complex property is required");
        }
        par.putIfAbsent("dbeg", "1800-01-01")
        par.putIfAbsent("dend", "3333-12-31")
        //
        if (par.getBoolean("isUniq"))
            validateDataUniq(par)

        updateTable("DataPropVal", par, par.getString("model"), par.getString("metamodel"))
        //
        Store stItems = loadSql("""
            select * from DataPropVal where parent=:id order by ord
        """, "DataPropVal", par.getString("model"), par.getString("metamodel"))

        for (StoreRecord rr : stItems) {
            rr.set("dbeg", par.getDate("dbeg"))
            rr.set("dend", par.getDate("dend"))
            rr.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
            updateTable("DataPropVal", rr.getValues(), par.getString("model"), par.getString("metamodel"))
        }
    }

    @DaoMethod
    Store loadClsFromChar() throws Exception {
        return loadSqlMeta("""
            select c.id, v.name from Cls c, ClsVer v 
            where c.id=v.ownerVer and v.lastVer=1
                and c.typ in (select typ from TypCharGr)
        """, "")
    }

    @DaoMethod
    Store loadRelTypFromChar() throws Exception {
        return loadSqlMeta("""
            select c.id, v.name from RelTyp c, RelTypVer v 
            where c.id=v.ownerVer and v.lastVer=1
                and c.id in (select reltyp from RelTypCharGr)
        """, "")
    }

    private long createOwner(Map<String, Object> params, String model, String metamodel) {
        if (metamodel == "fish") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).createOwner(params)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).createOwner(params)
            else if (model.equalsIgnoreCase("monitoringdata"))
                return apiMonitoringData().get(ApiMonitoringData).createOwner(params)
            else
                throw new XError("Unknown model [${model}]")
        } else if (metamodel == "dtj") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).createOwner(params)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).createOwner(params)
            else
                throw new XError("Unknown model [${model}]")
        }
        throw new XError("Unknown id metamodel")
    }

    private long getDataProp(Map<String, Object> map, Map<String, Object> reqParams) throws Exception {
        IVariantMap r = new VariantMap(map)
        String whe = " periodType is null"
        Map<String, Object> mapDP = new HashMap<>()
        if (r.getLong("periodType") > 0) {
            mapDP.put("periodType", r.getLong("periodType"))
            whe = " periodType=:periodType"
        }
        if (r.getLong("status") > 0) {
            mapDP.put("status", r.getLong("status"))
            whe += " and status=:status"
        } else {
            whe += " and status is null"
        }

        if (r.getLong("provider") > 0) {
            mapDP.put("provider", r.getLong("provider"))
            whe += " and provider=:provider"
        } else {
            whe += " and provider is null"
        }
        whe += " and objorrelobj=:owner and prop=:prop and isObj=:isObj"
        mapDP.put("objorrelobj", UtCnv.toLong(reqParams.get("owner")))
        mapDP.put("isObj", UtCnv.toInt(reqParams.get("isObj")))
        mapDP.put("prop", UtCnv.toLong(reqParams.get("prop")))

        Store st = loadSqlWithParams("""
            select id from DataProp where ${whe} 
            """, "", map, UtCnv.toString(reqParams.get("model")), UtCnv.toString(reqParams.get("metamodel")))
        if (st.size() > 0)
            return st.get(0).getLong("id")
        else
            return insertRecToTable("DataProp", mapDP, UtCnv.toString(reqParams.get("model")),
                    UtCnv.toString(reqParams.get("metamodel")), true)

    }

    private void validateDataUniq(Map<String, Object> params) throws Exception {
        IVariantMap map = new VariantMap(params)
        String mode = map.getString("mode")
        long status = map.getLong("status")
        long provider = map.getLong("provider")
        long periodType = map.getLong("periodType")
        String whePeriod = " and d.periodType is null and :dbeg < v.dend and :dend > v.dbeg"
        if (periodType > 0)
            whePeriod = " and d.periodType=:periodType and v.dbeg=:dbeg and v.dend=:dend"
        String wheStatus = " and d.status is null"
        if (status > 0)
            wheStatus = " and d.status=:status"
        String wheProvider = " and d.provider is null"
        if (provider > 0)
            wheProvider = " and d.provider=:provider"

        String sql = """
                    select v.id from DataProp d, DataPropVal v
                    where d.id=v.dataProp and d.objorrelobj=:owner and d.isObj=:isObj and d.prop=:prop
                """ + whePeriod + wheStatus + wheProvider

        if (mode == "upd")
            sql = sql + " and v.id <> :dataPropVal"
        if (map.getBoolean("isComplex"))
            sql = sql + " and v.parent is not null"

        if (map.getBoolean("isMulti")) {
            sql = """
                select v.id from DataMultiProp d, DataMultiPropCell v, DataMultiPropCellCoord c
                where d.id=v.dataMultiProp and d.objorrelobj=:owner and d.isObj=:isObj and d.multiProp=:multiProp
                    and v.id=c.datamultipropcell and c.multipropdim=:multiPropDim and c.dimmultipropitem=:dimMultiPropItem
            """ + whePeriod + wheStatus + wheProvider
            if (mode == "upd")
                sql = sql + " and v.id <> :id"
        }

        Store st = loadSqlWithParams(sql, "", map, map.getString("model"), map.getString("metamodel"))
        if (st.size() > 0)
            throw new XError("Интервалы жизни пересекаются")
    }

    private updateTable(String tableName, Map<String, Object> params, String model, String metamodel) {
        if (metamodel == "fish") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).updateTable(tableName, params)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).updateTable(tableName, params)
            else if (model.equalsIgnoreCase("monitoringdata"))
                return apiMonitoringData().get(ApiMonitoringData).updateTable(tableName, params)
            else
                throw new XError("Unknown model [${model}]")
        } else if (metamodel == "dtj") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).updateTable(tableName, params)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).updateTable(tableName, params)
            else
                throw new XError("Unknown model [${model}]")
        }
        throw new XError("Unknown id metamodel")
    }

    private deleteTable(String tableName, long id, String model, String metamodel) {
        if (metamodel == "fish") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).deleteTable(tableName, id)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).deleteTable(tableName, id)
            else if (model.equalsIgnoreCase("monitoringdata"))
                return apiMonitoringData().get(ApiMonitoringData).deleteTable(tableName, id)
            else
                throw new XError("Unknown model [${model}]")
        } else if (metamodel == "dtj") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).deleteTable(tableName, id)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).deleteTable(tableName, id)
            else
                throw new XError("Unknown model [${model}]")
        }
        throw new XError("Unknown id metamodel")
    }

    private long insertRecToTable(String tableName, Map<String, Object> params, String model, String metamodel, boolean generateId) {
        if (metamodel == "fish") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).insertRecToTable(tableName, params, generateId)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).insertRecToTable(tableName, params, generateId)
            else if (model.equalsIgnoreCase("monitoringdata"))
                return apiMonitoringData().get(ApiMonitoringData).insertRecToTable(tableName, params, generateId)
            else
                throw new XError("Unknown model [${model}]")
        } else if (metamodel == "dtj") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).insertRecToTable(tableName, params, generateId)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).insertRecToTable(tableName, params, generateId)
            else
                throw new XError("Unknown model [${model}]")
        }
        throw new XError("Unknown id metamodel")
    }

    private long insertRecToTable2(String tableName, Map<String, Object> params, String model, String metamodel, boolean generateId) {
        if (metamodel == "fish") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).insertRecToTable(tableName, params, generateId)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).insertRecToTable(tableName, params, generateId)
            else if (model.equalsIgnoreCase("monitoringdata"))
                return apiMonitoringData().get(ApiMonitoringData).insertRecToTable(tableName, params, generateId)
            else
                throw new XError("Unknown model [${model}]")
        } else if (metamodel == "dtj") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).insertRecToTable(tableName, params, generateId)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).insertRecToTable(tableName, params, generateId)
            else
                throw new XError("Unknown model [${model}]")
        }
        throw new XError("Unknown id metamodel")
    }

    private long getUser() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        long au = authSvc.getCurrentUser().getAttrs().getLong("id")
        if (au == 0)
            throw new XError("notLogined")
        return au
    }

    private static double round(double value, int places) {
        if (places < 0) throw new IllegalArgumentException()
        BigDecimal bd = new BigDecimal(Double.toString(value))
        bd = bd.setScale(places, RoundingMode.HALF_UP)
        return bd.doubleValue()
    }

    private void execSql(String sql, String model) {
        if (model.equalsIgnoreCase("userdata"))
            apiUserData().get(ApiUserData).execSql(sql)
        else if (model.equalsIgnoreCase("nsidata"))
            apiNSIData().get(ApiNSIData).execSql(sql)
        else if (model.equalsIgnoreCase("monitoringdata"))
            apiMonitoringData().get(ApiMonitoringData).execSql(sql)
        else
            throw new XError("Unknown model [${model}]")
    }

    private Store loadSqlMeta(String sql, String domain) {
        return apiMeta().get(ApiMeta).loadSql(sql, domain)
    }

    private Store loadSql(String sql, String domain, String model, String metamodel) {
        if (metamodel == "fish") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).loadSql(sql, domain)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).loadSql(sql, domain)
            else if (model.equalsIgnoreCase("monitoringdata"))
                return apiMonitoringData().get(ApiMonitoringData).loadSql(sql, domain)
            else
                throw new XError("Unknown model [${model}]")
        } else if (metamodel == "dtj") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).loadSql(sql, domain)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).loadSql(sql, domain)
            else
                throw new XError("Unknown model [${model}]")
        }
        throw new XError("Unknown id metamodel")
    }

    private Store loadSqlWithParams(String sql, String domain, Map<String, Object> params, String model, String metamodel) {
        if (metamodel == "fish") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).loadSqlWithParams(sql, params, domain)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).loadSqlWithParams(sql, params, domain)
            else if (model.equalsIgnoreCase("monitoringdata"))
                return apiMonitoringData().get(ApiMonitoringData).loadSqlWithParams(sql, params, domain)
            else
                throw new XError("Unknown model [${model}]")
        } else if (metamodel == "dtj") {
            if (model.equalsIgnoreCase("userdata"))
                return apiUserData().get(ApiUserData).loadSqlWithParams(sql, params, domain)
            else if (model.equalsIgnoreCase("nsidata"))
                return apiNSIData().get(ApiNSIData).loadSqlWithParams(sql, params, domain)
            else
                throw new XError("Unknown model [${model}]")
        }
        throw new XError("Unknown id metamodel")
    }


}
