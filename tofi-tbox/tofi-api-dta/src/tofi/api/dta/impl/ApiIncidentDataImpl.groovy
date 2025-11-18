package tofi.api.dta.impl

import jandcode.commons.UtCnv
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.VariantMap
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiIncidentData
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.dta.model.utils.UtPeriod
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_AttribValType_consts
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.api.mdl.model.consts.FD_PeriodType_consts
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class ApiIncidentDataImpl extends BaseMdbUtils implements ApiIncidentData {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
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
    long saveIncident(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)

        Map<String, Object> par = new HashMap<>(pms)
        String codCls = pms.getString("codCls")
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", codCls, "")
        if (map.isEmpty())
            throw new XError("NotFoundCod@${codCls}")
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        par.put("cls", map.get(codCls))
        par.put("fullName", par.get("name"))
        long own = eu.insertEntity(par)
        pms.put("own", own)

        //
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_StatusRegistered", "")
        long idFV_StatusRegistered = map.get("FV_StatusRegistered")
        long pvStatus = apiMeta().get(ApiMeta).idPV("factorVal", idFV_StatusRegistered, "Prop_Status")

        //1 Prop_Status
        pms.put("fvStatus", idFV_StatusRegistered)
        pms.put("pvStatus", pvStatus)
        fillProperties(true, "Prop_Status", pms)

        //3 Prop_Event
        if (pms.getLong("objEvent") > 0)
            fillProperties(true, "Prop_Event", pms)

        //4 Prop_Object
        if (pms.getLong("objObject") > 0)
            fillProperties(true, "Prop_Object", pms)

        //5 Prop_User
        if (pms.getLong("objUser") > 0)
            fillProperties(true, "Prop_User", pms)

        //6 Prop_ParameterLog
        if (pms.getLong("objParameterLog") > 0)
            fillProperties(true, "Prop_ParameterLog", pms)

        //7 Prop_Fault
        if (pms.getLong("objFault") > 0)
            fillProperties(true, "Prop_Fault", pms)

        //8 Prop_StartKm
        if (pms.getInt("StartKm") > 0)
            fillProperties(true, "Prop_StartKm", pms)

        //9 Prop_FinishKm
        if (pms.getInt("FinishKm") > 0)
            fillProperties(true, "Prop_FinishKm", pms)

        //10 Prop_StartPicket
        if (pms.getInt("StartPicket") > 0)
            fillProperties(true, "Prop_StartPicket", pms)

        //11 Prop_FinishPicket
        if (pms.getInt("FinishPicket") > 0)
            fillProperties(true, "Prop_FinishPicket", pms)

        //12 Prop_StartLink
        if (pms.getInt("StartLink") > 0)
            fillProperties(true, "Prop_StartLink", pms)

        //13 Prop_FinishLink
        if (pms.getInt("FinishLink") > 0)
            fillProperties(true, "Prop_FinishLink", pms)

        //14 Prop_CreatedAt
        if (pms.getString("CreatedAt") != "")
            fillProperties(true, "Prop_CreatedAt", pms)
        else
            throw new XError("[CreatedAt] not specified")

        //15 Prop_UpdatedAt
        if (pms.getString("UpdatedAt") != "")
            fillProperties(true, "Prop_UpdatedAt", pms)
        else
            throw new XError("[UpdatedAt] not specified")

        //16 Prop_RegistrationDateTime
        if (pms.getString("RegistrationDateTime") != "")
            fillProperties(true, "Prop_RegistrationDateTime", pms)
        else
            throw new XError("[RegistrationDateTime] not specified")

        //17 Prop_Description
        if (pms.getString("Description") != "")
            fillProperties(true, "Prop_Description", pms)
        else
            throw new XError("[Description] not specified")

        //18 Prop_InfoApplicant
        if (pms.getString("InfoApplicant") != "")
            fillProperties(true, "Prop_InfoApplicant", pms)
        else
            throw new XError("[InfoApplicant] not specified")

        return own
    }

    @Override
    long updateIncident(String mode, Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("id"))
        params.put("own", own)

        if (mode.equalsIgnoreCase("ins")) {
            if (params.get("idStatus") > 0) {
                Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_StatusWorkAssigned", "")
                long fvStatus = map.get("FV_StatusWorkAssigned")
                long pvStatus = apiMeta().get(ApiMeta).idPV("factorVal", fvStatus, "Prop_Status")
                params.put("pvStatus", pvStatus)
                params.put("fvStatus", fvStatus)
                updateProperties("Prop_Status", params)
            }

            if (params.get("AssignDateTime") != "")
                fillProperties(true, "Prop_AssignDateTime", params)

            if (params.get("objLocationClsSection") > 0)
                fillProperties(true, "Prop_LocationClsSection", params)

            if (params.get("fvCriticality") > 0)
                fillProperties(true, "Prop_Criticality", params)
        } else if (mode.equalsIgnoreCase("upd")) {
            if (params.containsKey("idStatus")) {
                if (UtCnv.toLong(params.get("fvStatus")) > 0)
                    updateProperties("Prop_Status", params)
                else
                    throw new XError("[Status] не указан")
            } else {
                throw new XError("[Status] не указан")
            }

        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }

        return own
    }

    private void fillProperties(boolean isObj, String cod, Map<String, Object> params) {
        long own = UtCnv.toLong(params.get("own"))
        String keyValue = cod.split("_")[1]
        def objRef = UtCnv.toLong(params.get("obj" + keyValue))
        def propVal = UtCnv.toLong(params.get("pv" + keyValue))

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
        // Attrib str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_InfoApplicant")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // Attrib str multiStr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_Description")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (params.get(keyValue) != null) {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // Attrib str dttm
        if ([FD_AttribValType_consts.dttm].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_RegistrationDateTime") ||
                    cod.equalsIgnoreCase("Prop_AssignDateTime")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Criticality") ||
                    cod.equalsIgnoreCase("Prop_Status")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_ParamsMeasure")) {
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
                    cod.equalsIgnoreCase("Prop_FinishKm") ||
                    cod.equalsIgnoreCase("Prop_StartPicket") ||
                    cod.equalsIgnoreCase("Prop_FinishPicket") ||
                    cod.equalsIgnoreCase("Prop_StartLink") ||
                    cod.equalsIgnoreCase("Prop_FinishLink")) {
                if (params.get(keyValue) != null) {
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
            if (cod.equalsIgnoreCase("Prop_Event") ||
                    cod.equalsIgnoreCase("Prop_Object") ||
                    cod.equalsIgnoreCase("Prop_User") ||
                    cod.equalsIgnoreCase("Prop_ParameterLog") ||
                    cod.equalsIgnoreCase("Prop_Fault")||
                    cod.equalsIgnoreCase("Prop_LocationClsSection") ||
                    cod.equalsIgnoreCase("Prop_WorkPlan")) {
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

        long au = 1
        recDPV.set("authUser", au)
        recDPV.set("inputType", FD_InputType_consts.app)
        long idDPV = mdb.getNextId("DataPropVal")
        recDPV.set("id", idDPV)
        recDPV.set("ord", idDPV)
        recDPV.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
        mdb.insertRec("DataPropVal", recDPV, false)
    }

    private void updateProperties(String cod, Map<String, Object> params) {
        VariantMap mapProp = new VariantMap(params)
        String keyValue = cod.split("_")[1]
        long idVal = mapProp.getLong("id" + keyValue)
        long propVal = mapProp.getLong("pv" + keyValue)
        Store stProp = apiMeta().get(ApiMeta).getPropInfo(cod)
        long propType = stProp.get(0).getLong("propType")
        String sql = ""
        def tmst = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)
        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Status")) {
                if (propVal > 0)
                    sql = "update DataPropval set propVal=${propVal}, timeStamp='${tmst}' where id=${idVal}"
                else {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        mdb.execQueryNative(sql)
    }
}
