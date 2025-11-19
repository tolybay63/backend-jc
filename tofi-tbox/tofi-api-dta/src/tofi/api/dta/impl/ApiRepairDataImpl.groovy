package tofi.api.dta.impl

import jandcode.commons.datetime.XDateTime
import jandcode.commons.error.XError
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import tofi.api.dta.ApiClientData
import tofi.api.dta.ApiIncidentData
import tofi.api.dta.ApiInspectionData
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.dta.ApiOrgStructureData
import tofi.api.dta.ApiPersonnalData
import tofi.api.dta.ApiPlanData
import tofi.api.dta.ApiRepairData
import tofi.api.dta.ApiResourceData
import tofi.api.dta.ApiUserData
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiRepairDataImpl extends BaseMdbUtils implements ApiRepairData {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }
    ApinatorApi apiIncidentData() {return app.bean(ApinatorService).getApi("incidentdata")}
    ApinatorApi apiPlanData() {
        return app.bean(ApinatorService).getApi("plandata")
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
    void checkStatusOfIncident(long objWorkPlan, String codStatusFrom, String codStatusTo) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "Prop_Incident", "")
        Store stPlan = loadSqlService("""
                select v1.obj
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Incident")}
                    inner join DataPropVal v1 on d1.id=v1.dataProp
                where o.id=${objWorkPlan}    
            """, "", "plandata")
        if (stPlan.size()>0) {
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
            Store stIncident = loadSqlService("""
                    select v1.id, v1.propVal, v2.dateTimeVal
                    from Obj o
                        left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Status")}
                        inner join DataPropVal v1 on d1.id=v1.dataProp
                        left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_CloseDateTime")}
                        left join DataPropVal v2 on d2.id=v2.dataProp
                    where o.id=${stPlan.get(0).getLong("obj")}    
                """, "", "incidentdata")
            //
            map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "", "FV_Status%")
            long fvStatus = map.get(codStatusFrom)
            long pvStatus = apiMeta().get(ApiMeta).idPV("factorVal", fvStatus, "Prop_Status")
            if (pvStatus == stIncident.get(0).getLong("propVal")) {
                Map<String, Object> mapPar = new HashMap<>()
                if (codStatusTo == "FV_StatusEliminated" && stIncident.get(0).getString("dateTimeVal").startsWith("0000-01-01"))
                    mapPar.put("CloseDateTime", XDateTime.now())
                //
                fvStatus = map.get(codStatusTo)
                pvStatus = apiMeta().get(ApiMeta).idPV("factorVal", fvStatus, "Prop_Status")
                //

                mapPar.put("id", stPlan.get(0).getLong("obj"))
                mapPar.put("idStatus", stIncident.get(0).getLong("id"))
                mapPar.put("fvStatus", fvStatus)
                mapPar.put("pvStatus", pvStatus)
                apiIncidentData().get(ApiIncidentData).updateIncident("upd", mapPar)
            }
        }
    }
    ////////
    private Store loadSqlService(String sql, String domain, String model) {
        if (model.equalsIgnoreCase("plandata"))
            return apiPlanData().get(ApiPlanData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
        else
            throw new XError("Unknown model [${model}]")
    }


}
