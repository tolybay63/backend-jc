package tofi.api.dta.impl

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiClientData
import tofi.api.dta.ApiIncidentData
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
import tofi.api.dta.ApiRepairData
import tofi.api.dta.ApiReportData
import tofi.api.dta.ApiResourceData
import tofi.api.dta.ApiUserData
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.dta.model.utils.UtPeriod
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_AttribValType_consts
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.api.mdl.model.consts.FD_PeriodType_consts
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiObjectDataImpl extends BaseMdbUtils implements ApiObjectData {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }
    ApinatorApi apiUserData() {
        return app.bean(ApinatorService).getApi("userdata")
    }
    ApinatorApi apiNSIData() {
        return app.bean(ApinatorService).getApi("nsidata")
    }
    ApinatorApi apiObjectData() {
        return app.bean(ApinatorService).getApi("objectdata")
    }
    ApinatorApi apiPlanData() {
        return app.bean(ApinatorService).getApi("plandata")
    }
    ApinatorApi apiPersonnalData() {
        return app.bean(ApinatorService).getApi("personnaldata")
    }
    ApinatorApi apiOrgStructureData() {
        return app.bean(ApinatorService).getApi("orgstructuredata")
    }
    ApinatorApi apiInspectionData() {
        return app.bean(ApinatorService).getApi("inspectiondata")
    }
    ApinatorApi apiClientData() {
        return app.bean(ApinatorService).getApi("clientdata")
    }
    ApinatorApi apiIncidentData() {
        return app.bean(ApinatorService).getApi("incidentdata")
    }
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }
    ApinatorApi apiReportData() {
        return app.bean(ApinatorService).getApi("reportdata")
    }


    @Override
    Store loadSql(String sql, String domain) {
        if (domain.isEmpty())
            return mdb.loadQuery(sql)
        else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql)
        }
    }

    @Override
    Store loadSqlWithParams(String sql, Map<String, Object> params, String domain) {
        if (domain.isEmpty())
            return mdb.loadQuery(sql, params)
        else {
            Store st = mdb.createStore(domain)
            return mdb.loadQuery(st, sql, params)
        }
    }

    @Override
    long getClsOrRelCls(long owner, int isObj) {
        if (isObj==1) {
            Store stTmp =  mdb.loadQuery("select cls from Obj where id=:id", [id: owner])
            if (stTmp.size()>0)
                return stTmp.get(0).getLong("cls")
            else
                return 0
        } else {
            Store stTmp =  mdb.loadQuery("select relcls from RelObj where id=:id", [id: owner])
            if (stTmp.size()>0)
                return stTmp.get(0).getLong("relcls")
            else
                return 0
        }
    }

    @Override
    boolean is_exist_entity_as_data(long entId, String entName, String propVal) {
        if (entName.equalsIgnoreCase("obj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=1 and v.propVal in (0${propVal}) and v.obj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("relobj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=0 and v.propVal in (0${propVal}) and v.relobj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("factorVal")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and v.propVal=${propVal} and v.obj is null and v.relobj is null
                limit 1
            """).size() > 0
        }
        throw new XError("Not known Entity")
    }

    @Override
    boolean is_exist_entity_as_dataOld(long entId, String entName, long propVal) {
        if (entName.equalsIgnoreCase("obj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=1 and v.propVal=${propVal} and v.obj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("relobj")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and d.isObj=0 and v.propVal=${propVal} and v.relobj=${entId}
                limit 1
            """).size() > 0
        } else if (entName.equalsIgnoreCase("factorVal")) {
            return mdb.loadQuery("""
                select v.id from DataProp d, DataPropVal v
                where d.id=v.dataProp and v.propVal=${propVal} and v.obj is null and v.relobj is null
                limit 1
            """).size() > 0
        }
        throw new XError("Not known Entity")
    }

    @Override
    boolean checkExistOwners(long clsORrelcls, boolean isObj) {
        if (isObj)
            return mdb.loadQueryNative("""
                select id from Obj where cls=${clsORrelcls} limit 1
            """).size() > 0
        else
            return mdb.loadQueryNative("""
                select id from RelObj where relcls=${clsORrelcls} limit 1
            """).size() > 0
    }

    @Override
    long insertRecToTable(String tableName, Map<String, Object> params, boolean generateId) {
        Store st = mdb.createStore(tableName)
        StoreRecord r = st.add(params)
        if (generateId)
            return mdb.insertRec(tableName, r, generateId)
        else {
            long id = mdb.getNextId(tableName)
            r.set("id", id)
            r.set("ord", id)
            r.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
            //
            return mdb.insertRec(tableName, r, false)
        }
    }

    @Override
    void updateTable(String tableName, Map<String, Object> params) {
        Store st = mdb.createStore(tableName)
        StoreRecord r = st.add(params)
        mdb.updateRec(tableName, r)
    }

    @Override
    void execSql(String sql) {
        mdb.execQueryNative(sql)
    }

    @Override
    long createOwner(Map<String, Object> params) {
        String tabl = "RelObj"
        if (UtCnv.toBoolean(params.get("isObj")))
            tabl = "Obj"
        EntityMdbUtils ent = new EntityMdbUtils(mdb, tabl)
        if (UtCnv.toString(params.get("mode"))=="ins")
            return ent.insertEntity(params)
        else {
            ent.updateEntity(params)
            return UtCnv.toLong(params.get("id"))
        }
    }

    @Override
    void deleteOwnerWithProperties(long id, int isObj) {
        //
        Store stObj = mdb.loadQuery("""
            select o.id
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and v.objParent=${id}
        """)
        if (stObj.size()>0)
            throw new XError("Существуют дочерние элементы")

        validateForDeleteOwner(id, isObj)
        //
        String tableName = isObj==1 ? "Obj" : "RelObj"
        EntityMdbUtils eu = new EntityMdbUtils(mdb, tableName)
        mdb.execQueryNative("""
            delete from DataPropVal
            where dataProp in (select id from DataProp where isobj=${isObj} and objorrelobj=${id});
            delete from DataProp where id in (
                select id from dataprop
                except
                select dataProp as id from DataPropVal
            );
        """)
        if (tableName.equalsIgnoreCase("RelObj")) {
            try {
                mdb.execQueryNative("""
                    delete from RelObjMember
                    where relobj=${id};
                """)
            } finally {
                eu.deleteEntity(id)
            }
        } else
            eu.deleteEntity(id)
    }

    @Override
    Store loadObjList(String codClsOrTyp, String codProp, String model) {
        Map<String, Long> map
        String sql
        if (codClsOrTyp.startsWith("Cls_")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", codClsOrTyp, "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@${codClsOrTyp}")
            sql = """
                select o.id, o.cls, v.name, v.fullname, null as pv 
                from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls=${map.get(codClsOrTyp)}
            """
        } else if (codClsOrTyp.startsWith("Typ_")) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Typ", codClsOrTyp, "")
            if (map.isEmpty())
                throw new XError("NotFoundCod@${codClsOrTyp}")
            Store stTmp = loadSqlMeta("""
                select id from Cls where typ=${map.get(codClsOrTyp)}
            """, "")
            Set<Object> idsCls = stTmp.getUniqueValues("id")

            sql = """
                select o.id, o.cls, v.name, v.fullname, null as pv 
                from Obj o, ObjVer v
                where o.id=v.ownerVer and v.lastVer=1 and o.cls in (${idsCls.join(",")})
            """
        } else
            throw new XError("Неисвезстная сущность")
        Store st = loadSqlService(sql, "", model)
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", codProp, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codProp}")

        Store stPV = loadSqlMeta("""
            select id, cls  from propval p where prop=${map.get(codProp)}
        """, "")
        StoreIndex indPV = stPV.getIndex("cls")

        for (StoreRecord r in st) {
            StoreRecord rec = indPV.get(r.getLong("cls"))
            if (rec != null)
                r.set("pv", rec.getLong("id"))
        }

        Store res = mdb.createStore("Obj.ObjList")
        for (StoreRecord r in st) {
            if (r.getLong("pv") > 0)
                res.add(r)
        }
        return res
    }

    @Override
    void fillProperties(boolean isObj, String cod, Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("own"))
        String keyValue = cod.split("_")[1]
        def objRef = UtCnv.toLong(params.get("obj"+keyValue))
        def propVal = UtCnv.toLong(params.get("pv"+keyValue))

        Store stProp = apiMeta().get(ApiMeta).getPropInfo(cod)
        //
        long prop = stProp.get(0).getLong("id")
        long propType = stProp.get(0).getLong("propType")
        long attribValType = stProp.get(0).getLong("attribValType")
        Integer digit = null
        double koef = stProp.get(0).getDouble("koef")
        if (koef == 0) koef = 1
        if (!stProp.get(0).isValueNull("digit"))
            digit = stProp.get(0).getInt("digit")

        //
        long idDP
        StoreRecord recDP = mdb.createStoreRecord("DataProp")
        String whe = isObj ? "and isObj=1 " : "and isObj=0 "
        if (stProp.get(0).getLong("statusFactor") > 0) {
            long fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
            whe += "and status = ${fv} "
        } else {
            whe += "and status is null "
        }
        whe += "and provider is null "
        //todo if (stProp.get(0).getLong("providerTyp") > 0)

        if (stProp.get(0).getLong("providerTyp") > 0) {
            whe += "and periodType is not null "
        } else {
            whe += "and periodType is null"
        }
        Store stDP = mdb.loadQuery("""
            select * from DataProp
            where objOrRelObj=${own} and prop=${prop} ${whe}
        """)
        if (stDP.size() > 0) {
            idDP = stDP.get(0).getLong("id")
        } else {
            recDP.set("isObj", isObj)
            recDP.set("objOrRelObj", own)
            recDP.set("prop", prop)
            if (stProp.get(0).getLong("statusFactor") > 0) {
                long fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
                recDP.set("status", fv)
            }
            if (stProp.get(0).getLong("providerTyp") > 0) {
                //todo
                // provider
                //
            }
            if (stProp.get(0).getBoolean("dependperiod")) {
                recDP.set("periodType", FD_PeriodType_consts.year)
            }
            idDP = mdb.insertRec("DataProp", recDP, true)
        }
        //
        StoreRecord recDPV = mdb.createStoreRecord("DataPropVal")
        recDPV.set("dataProp", idDP)
        // Attrib
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_Specs") ||
                    cod.equalsIgnoreCase("Prop_LocationDetails") ||
                    cod.equalsIgnoreCase("Prop_Number")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if ( cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_InstallationDate")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_Side") ) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if ( cod.equalsIgnoreCase("Prop_ParamsMeasure")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_StartKm") ||
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                    cod.equalsIgnoreCase("Prop_PeriodicityReplacement")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    double v = UtCnv.toDouble(params.get(keyValue))
                    v = v / koef
                    if (digit) v = v.round(digit)
                    recDPV.set("numberval", v)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // For Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_ObjectType") ||
                    cod.equalsIgnoreCase("Prop_Section")) {
                if (objRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("obj", objRef)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //
        if (recDP.getLong("periodType") > 0) {
            if (!params.containsKey("dte"))
                params.put("dte", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(params.get("dte")), recDP.getLong("periodType"), 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(params.get("dte")), recDP.getLong("periodType"), 0)
            recDPV.set("dbeg", d1.toString(XDateTimeFormatter.ISO_DATE))
            recDPV.set("dend", d2.toString(XDateTimeFormatter.ISO_DATE))
        } else {
            recDPV.set("dbeg", "1800-01-01")
            recDPV.set("dend", "3333-12-31")
        }

        //long au = getUser()
        recDPV.set("authUser", 1)
        recDPV.set("inputType", FD_InputType_consts.app)
        long idDPV = mdb.getNextId("DataPropVal")
        recDPV.set("id", idDPV)
        recDPV.set("ord", idDPV)
        recDPV.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
        mdb.insertRec("DataPropVal", recDPV, false)
    }

    void validateForDeleteOwner(long owner, int isObj) {
        //---< check data in other DB
        if (isObj==1) {
            Store stObj = mdb.loadQuery("""
                select o.cls, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${owner}
             """)
            if (stObj.size() > 0) {
                //
                List<String> lstService = new ArrayList<>()
                long cls = stObj.get(0).getLong("cls")
                String name = stObj.get(0).getString("name")
                Store stPV = loadSqlMeta("""
                    select id from PropVal where cls=${cls}
                """, "")
                Set<Object> idsPV = stPV.getUniqueValues("id")
                if (stPV.size() > 0) {
                    Store stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and obj=${owner}
                    """, "", "nsidata")
                    if (stData.size() > 0)
                        lstService.add("nsidata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and obj=${owner}
                    """, "", "objectdata")
                    if (stData.size() > 0)
                        lstService.add("objectdata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and obj=${owner}
                    """, "", "orgstructuredata")
                    if (stData.size() > 0)
                        lstService.add("orgstructuredata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and obj=${owner}
                    """, "", "personnaldata")
                    if (stData.size() > 0)
                        lstService.add("personnaldata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and obj=${owner}
                    """, "", "plandata")
                    if (stData.size() > 0)
                        lstService.add("plandata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and obj=${owner}
                    """, "", "inspectiondata")
                    if (stData.size() > 0)
                        lstService.add("inspectiondata")

                    //
                    if (lstService.size() > 0) {
                        throw new XError("${name} используется в [" + lstService.join(", ") + "]")
                    }
                }
            }
        } else if (isObj==0) {
            Store stRelObj = mdb.loadQuery("""
                select o.relcls, v.name from RelObj o, RelObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${owner}
             """)
            if (stRelObj.size() > 0) {
                //
                List<String> lstService = new ArrayList<>()
                long relcls = stRelObj.get(0).getLong("relcls")
                String name = stRelObj.get(0).getString("name")
                Store stPV = loadSqlMeta("""
                    select id from PropVal where cls=${relcls}
                """, "")
                Set<Object> idsPV = stPV.getUniqueValues("id")
                if (stPV.size() > 0) {
                    Store stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "nsidata")
                    if (stData.size() > 0)
                        lstService.add("nsidata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "objectdata")
                    if (stData.size() > 0)
                        lstService.add("objectdata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "orgstructuredata")
                    if (stData.size() > 0)
                        lstService.add("orgstructuredata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "personnaldata")
                    if (stData.size() > 0)
                        lstService.add("personnaldata")
                    //
                    stData = loadSqlService("""
                        select id from DataPropVal
                        where propval in (${idsPV.join(",")}) and relobj=${owner}
                    """, "", "plandata")
                    if (stData.size() > 0)
                        lstService.add("plandata")
                    if (lstService.size() > 0) {
                        throw new XError("${name} используется в [" + lstService.join(", ") + "]")
                    }
                }
            }
        } else {
            throw new XError("isObj is wrong")
        }
    }

    private Store loadSqlMeta(String sql, String domain) {
        return apiMeta().get(ApiMeta).loadSql(sql, domain)
    }

    private Store loadSqlService(String sql, String domain, String model) {
        if (model.equalsIgnoreCase("userdata"))
            return apiUserData().get(ApiUserData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("nsidata"))
            return apiNSIData().get(ApiNSIData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("objectdata"))
            return apiObjectData().get(ApiObjectData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("plandata"))
            return apiPlanData().get(ApiPlanData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("personnaldata"))
            return apiPersonnalData().get(ApiPersonnalData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("orgstructuredata"))
            return apiOrgStructureData().get(ApiOrgStructureData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("inspectiondata"))
            return apiInspectionData().get(ApiInspectionData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("clientdata"))
            return apiClientData().get(ApiClientData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
            return apiResourceData().get(ApiResourceData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("repairdata"))
            return apiRepairData().get(ApiRepairData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("reportdata"))
            return apiReportData().get(ApiReportData).loadSql(sql, domain)
        else
            throw new XError("Unknown model [${model}]")
    }



}
