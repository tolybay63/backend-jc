package dtj.report.dao


import groovy.transform.CompileStatic
import jandcode.commons.UtCnv
import jandcode.commons.UtDateTime
import jandcode.commons.UtFile
import jandcode.commons.datetime.XDate
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.commons.variant.VariantMap
import jandcode.core.auth.AuthService
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import org.apache.poi.ss.usermodel.BorderStyle
import org.apache.poi.ss.usermodel.Cell
import org.apache.poi.ss.usermodel.RangeCopier
import org.apache.poi.ss.usermodel.Row
import org.apache.poi.ss.util.CellRangeAddress
import org.apache.poi.xssf.usermodel.*
import tofi.api.dta.*
import tofi.api.dta.model.utils.EntityMdbUtils
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.model.consts.FD_AttribValType_consts
import tofi.api.mdl.model.consts.FD_InputType_consts
import tofi.api.mdl.model.consts.FD_PeriodType_consts
import tofi.api.mdl.model.consts.FD_PropType_consts
import tofi.api.mdl.utils.UtPeriod
import tofi.api.mdl.utils.dimPeriod.PeriodGenerator
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

@CompileStatic
class ReportDao extends BaseMdbUtils {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }
    ApinatorApi apiUserData() {
        return app.bean(ApinatorService).getApi("userdata")
    }
    ApinatorApi apiNSIData() {
        return app.bean(ApinatorService).getApi("nsidata")
    }
    ApinatorApi apiPersonnalData() {
        return app.bean(ApinatorService).getApi("personnaldata")
    }
    ApinatorApi apiOrgStructureData() {
        return app.bean(ApinatorService).getApi("orgstructuredata")
    }
    ApinatorApi apiObjectData() {
        return app.bean(ApinatorService).getApi("objectdata")
    }
    ApinatorApi apiPlanData() {
        return app.bean(ApinatorService).getApi("plandata")
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
    ApinatorApi apiRepairData() {
        return app.bean(ApinatorService).getApi("repairdata")
    }
    ApinatorApi apiResourceData() {
        return app.bean(ApinatorService).getApi("resourcedata")
    }
    //=========================================================================

    @DaoMethod
    Store loadReportConfiguration(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ReportConfiguration", "")
        Store st = mdb.createStore("Report.ReportConfiguration")

        String whe
        if (id > 0)
            whe = "o.id=${id}"
        else {
            whe = "o.cls = ${map.get("Cls_ReportConfiguration")}"
        }
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name, v.objparent as parent,
                v1.id as idFilter, v1.multiStrVal as Filter,
                v2.id as idRow, v2.multiStrVal as Row,
                v3.id as idCol, v3.multiStrVal as Col,
                v4.id as idFilterVal, v4.multiStrVal as FilterVal,
                v5.id as idRowVal, v5.multiStrVal as RowVal,
                v7.id as idColVal, v7.multiStrVal as ColVal,
                v8.id as idRowTotal, v8.propVal as pvRowTotal, null as fvRowTotal, null as nameRowTotal,
                v9.id as idColTotal, v9.propVal as pvColTotal, null as fvColTotal, null as nameColTotal,
                v10.id as idMetricsComplex, v10.strVal as MetricsComplex,
                v11.id as idFieldName, v11.strVal as FieldName,
                v12.id as idFieldVal, v12.propVal as pvFieldVal, null as fvFieldVal, null as nameFieldVal,
                v13.id as idUser, v13.obj as objUser, v13.propVal as pvUser, null as fullNameUser,
                v14.id as idCreatedAt, v14.dateTimeVal as CreatedAt,
                v15.id as idUpdatedAt, v15.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_Filter
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Row
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_Col
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_FilterVal
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_RowVal
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_ColVal
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_RowTotal
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=:Prop_ColTotal
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d10 on d10.objorrelobj=o.id and d10.prop=:Prop_MetricsComplex
                left join DataPropVal v10 on d10.id=v10.dataprop
                left join DataProp d11 on d11.objorrelobj=o.id and d11.prop=:Prop_FieldName
                left join DataPropVal v11 on d11.id=v11.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=:Prop_FieldVal
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=:Prop_User
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=:Prop_CreatedAt
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=:Prop_UpdatedAt
                left join DataPropVal v15 on d15.id=v15.dataprop
            where ${whe}
        """, map)
        //Пересечение
        Set<Object> pvsRowTotal = st.getUniqueValues("pvRowTotal")
        Set<Object> pvsColTotal = st.getUniqueValues("pvColTotal")
        Set<Object> pvsFieldVal = st.getUniqueValues("pvFieldVal")
        Set<Object> pvs = new HashSet<>()
        pvs.addAll(pvsRowTotal)
        pvs.addAll(pvsColTotal)
        pvs.addAll(pvsFieldVal)

        Store stPV = apiMeta().get(ApiMeta).loadSql("""
            select fv.id as fv, pv.id as pv, fv.name from Factor fv, PropVal pv 
            where pv.factorval=fv.id and pv.id in (0${pvs.join(",")})
        """, "")
        StoreIndex indPV = stPV.getIndex("pv")
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
            StoreRecord rec = indPV.get(r.getLong("pvRowTotal"))
            if (rec != null) {
                r.set("fvRowTotal", rec.getLong("fv"))
                r.set("nameRowTotal", rec.getString("name"))
            }
            rec = indPV.get(r.getLong("pvColTotal"))
            if (rec != null) {
                r.set("fvColTotal", rec.getLong("fv"))
                r.set("nameColTotal", rec.getString("name"))
            }
            rec = indPV.get(r.getLong("pvFieldVal"))
            if (rec != null) {
                r.set("fvFieldVal", rec.getLong("fv"))
                r.set("nameFieldVal", rec.getString("name"))
            }
        }
        //
        return st
    }

    @DaoMethod
    Store saveReportConfiguration(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ReportConfiguration", "")
        if (mode.equalsIgnoreCase("ins")) {
            par.put("cls", map.get("Cls_ReportConfiguration"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)
            //1 Prop_Filter
            if (pms.getString("Filter").isEmpty())
                throw new XError("[Filter] не указан")
            else
                fillProperties(true, "Prop_Filter", pms)
            //2 Prop_Row
            if (pms.getString("Row").isEmpty())
                throw new XError("[Row] не указан")
            else
                fillProperties(true, "Prop_Row", pms)
            //3 Prop_Col
            if (pms.getString("Col").isEmpty())
                throw new XError("[Col] не указан")
            else
                fillProperties(true, "Prop_Col", pms)
            //4 Prop_FilterVal
            if (!pms.getString("FilterVal").isEmpty())
                fillProperties(true, "Prop_FilterVal", pms)
            //5 Prop_RowVal
            if (!pms.getString("RowVal").isEmpty())
                fillProperties(true, "Prop_RowVal", pms)
            //6 Prop_ColVal
            if (!pms.getString("ColVal").isEmpty())
                fillProperties(true, "Prop_ColVal", pms)
            //7 Prop_RowTotal
            if (pms.getLong("fvRowTotal") == 0)
                throw new XError("[RowTotal] не указан")
            else
                fillProperties(true, "Prop_RowTotal", pms)
            //8 Prop_ColTotal
            if (pms.getLong("fvColTotal") == 0)
                throw new XError("[ColTotal] не указан")
            else
                fillProperties(true, "Prop_ColTotal", pms)
            //9 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //10 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //11 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)
            //12 Complex
            pms.remove("idComplex")
            pms.put("MetricsComplex", "MetricsComplex-" + own + "-" + pms.getString("FieldName"))
            mdb.startTran()
            try {
                fillProperties(true, "Prop_MetricsComplex", pms)
                //
                if (!pms.getString("FieldName").isEmpty())
                    fillProperties(true, "Prop_FieldName", pms)
                else
                    throw new XError("[FieldName] не указан")
                //
                if (pms.getLong("fvFieldVal") > 0)
                    fillProperties(true, "Prop_FieldVal", pms)
                else
                    throw new XError("[FieldVal] не указан")
                mdb.commit()
            } catch (Exception e) {
                mdb.rollback(e)
            }
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //
            //1 Prop_Filter
            if (pms.containsKey("idFilter")) {
                if (pms.getString("Filter").isEmpty())
                    throw new XError("[Filter] не указан")
                else
                    updateProperties("Prop_Filter", pms)
            }
            //2 Prop_Row
            if (pms.containsKey("idRow")) {
                if (pms.getString("Row").isEmpty())
                    throw new XError("[Row] не указан")
                else
                    updateProperties("Prop_Row", pms)
            }
            //3 Prop_Col
            if (pms.containsKey("idCol")) {
                if (pms.getString("Col").isEmpty())
                    throw new XError("[Col] не указан")
                else
                    updateProperties("Prop_Col", pms)
            }
            //4 Prop_FilterVal
            if (pms.containsKey("idFilterVal")) {
                updateProperties("Prop_FilterVal", pms)
            } else {
                if (!pms.getString("FilterVal").isEmpty())
                    fillProperties(true, "Prop_FilterVal", pms)
            }
            //5 Prop_RowVal
            if (pms.containsKey("idRowVal")) {
                updateProperties("Prop_RowVal", pms)
            } else {
                if (!pms.getString("RowVal").isEmpty())
                    fillProperties(true, "Prop_RowVal", pms)
            }
            //6 Prop_ColVal
            if (pms.containsKey("idColVal")) {
                updateProperties("Prop_ColVal", pms)
            } else {
                if (!pms.getString("ColVal").isEmpty())
                    fillProperties(true, "Prop_ColVal", pms)
            }
            //7 Prop_RowTotal
            if (pms.containsKey("idRowTotal")) {
                if (pms.getLong("fvRowTotal") == 0)
                    throw new XError("[RowTotal] не указан")
                else
                    updateProperties("Prop_RowTotal", pms)
            }
            //8 Prop_ColTotal
            if (pms.containsKey("idColTotal")) {
                if (pms.getLong("fvColTotal") == 0)
                    throw new XError("[ColTotal] не указан")
                else
                    updateProperties("Prop_ColTotal", pms)
            }
            //9 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)
            //10 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
            //11 Prop_FieldName
            if (pms.containsKey("idFieldName"))
                if (pms.getString("FieldName").isEmpty())
                    throw new XError("[FieldName] не указан")
                else
                    updateProperties("Prop_FieldName", pms)
            //12 Prop_FieldVal
            if (pms.containsKey("idFieldVal"))
                if (pms.getLong("fvFieldVal") == 0)
                    throw new XError("[FieldVal] не указан")
                else
                    updateProperties("Prop_FieldVal", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadReportConfiguration(own)
    }

    @DaoMethod
    Store loadReportSource(long id) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ReportSource", "")
        Store st = mdb.createStore("Report.ReportSource")

        String whe
        if (id > 0)
            whe = "o.id=${id}"
        else {
            whe = "o.cls = ${map.get("Cls_ReportSource")}"
        }
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        mdb.loadQuery(st, """
            select o.id, o.cls, v.name,
                v1.id as idURL, v1.strVal as URL,
                v2.id as idMethod, v2.strVal as Method,
                v3.id as idMethodTyp, v3.propVal as pvMethodTyp, null as fvMethodTyp, null as nameMethodTyp,
                v4.id as idMethodBody, v4.multiStrVal as MethodBody,
                v5.id as idUser, v5.obj as objUser, v5.propVal as pvUser, null as fullNameUser,
                v7.id as idCreatedAt, v7.dateTimeVal as CreatedAt,
                v8.id as idUpdatedAt, v8.dateTimeVal as UpdatedAt
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=:Prop_URL
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=:Prop_Method
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=:Prop_MethodTyp
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=:Prop_MethodBody
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=:Prop_User
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=:Prop_CreatedAt
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=:Prop_UpdatedAt
                left join DataPropVal v8 on d8.id=v8.dataprop
            where ${whe}
        """, map)
        //Пересечение
        Map<Long, Long> mapFV = apiMeta().get(ApiMeta).mapEntityIdFromPV("factorVal", "Prop_MethodTyp", true)
        //
        Set<Object> idsUser = st.getUniqueValues("objUser")
        Store stUser = loadSqlService("""
            select o.id, o.cls, v.fullName
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsUser.join(",")})
        """, "", "personnaldata")
        StoreIndex indUser = stUser.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recUser = indUser.get(r.getLong("objUser"))
            if (recUser != null)
                r.set("fullNameUser", recUser.getString("fullName"))
            r.set("fvMethodTyp", mapFV.get(r.getLong("pvMethodTyp")))
        }
        //
        Set<Object> fvs = st.getUniqueValues("fvMethodTyp")
        Store stFV = apiMeta().get(ApiMeta).loadSql("""
            select id, name from Factor where id in (0${fvs.join(",")}) 
        """, "")
        StoreIndex indFV = stFV.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord rec = indFV.get(r.getLong("fvMethodTyp"))
            if (rec != null)
                r.set("nameMethodTyp", rec.getString("name"))
        }
        //
        return st
    }

    @DaoMethod
    Store saveReportSource(String mode, Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long own
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        if (UtCnv.toString(params.get("name")).trim().isEmpty())
            throw new XError("[name] не указан")
        Map<String, Object> par = new HashMap<>(pms)
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ReportSource", "")
        if (mode.equalsIgnoreCase("ins")) {
            par.put("cls", map.get("Cls_ReportSource"))
            //
            par.putIfAbsent("fullName", pms.getString("name"))
            //
            own = eu.insertEntity(par)
            pms.put("own", own)
            //
            //1 Prop_URL
            if (pms.getString("URL").isEmpty())
                throw new XError("[URL] не указан")
            else
                fillProperties(true, "Prop_URL", pms)
            //2 Prop_Method
            if (pms.getString("Method").isEmpty())
                throw new XError("[Method] не указан")
            else
                fillProperties(true, "Prop_Method", pms)
            //3 Prop_MethodTyp
            if (pms.getLong("fvMethodTyp") == 0)
                throw new XError("[MethodTyp] не указан")
            else
                fillProperties(true, "Prop_MethodTyp", pms)
            //4 Prop_MethodBody
            if (pms.getString("MethodBody").isEmpty())
                throw new XError("[MethodBody] не указан")
            else
                fillProperties(true, "Prop_MethodBody", pms)
            //5 Prop_User
            if (pms.getLong("objUser") == 0)
                throw new XError("[User] не указан")
            else
                fillProperties(true, "Prop_User", pms)
            //6 Prop_CreatedAt
            if (pms.getString("CreatedAt").isEmpty())
                throw new XError("[CreatedAt] не указан")
            else
                fillProperties(true, "Prop_CreatedAt", pms)
            //7 Prop_UpdatedAt
            if (pms.getString("UpdatedAt").isEmpty())
                throw new XError("[UpdatedAt] не указан")
            else
                fillProperties(true, "Prop_UpdatedAt", pms)
        } else if (mode.equalsIgnoreCase("upd")) {
            own = pms.getLong("id")
            par.putIfAbsent("fullName", pms.getString("name"))
            eu.updateEntity(par)
            //
            pms.put("own", own)
            //
            //1 Prop_URL
            if (pms.containsKey("idURL")) {
                if (pms.getString("URL").isEmpty())
                    throw new XError("[URL] не указан")
                else
                    updateProperties("Prop_URL", pms)
            }
            //2 Prop_Method
            if (pms.containsKey("idMethod")) {
                if (pms.getString("Method").isEmpty())
                    throw new XError("[Method] не указан")
                else
                    updateProperties("Prop_Method", pms)
            }
            //3 Prop_MethodTyp
            if (pms.containsKey("idMethodTyp")) {
                if (pms.getLong("fvMethodTyp") == 0)
                    throw new XError("[MethodTyp] не указан")
                else
                    updateProperties("Prop_MethodTyp", pms)
            }
            //4 Prop_MethodBody
            if (pms.containsKey("idMethodBody")) {
                if (pms.getString("MethodBody").isEmpty())
                    throw new XError("[MethodBody] не указан")
                else
                    updateProperties("Prop_MethodBody", pms)
            }
            //5 Prop_User
            if (pms.containsKey("idUser"))
                if (pms.getLong("objUser") == 0)
                    throw new XError("[User] не указан")
                else
                    updateProperties("Prop_User", pms)
            //6 Prop_UpdatedAt
            if (pms.containsKey("idUpdatedAt"))
                if (pms.getString("UpdatedAt").isEmpty())
                    throw new XError("[UpdatedAt] не указан")
                else
                    updateProperties("Prop_UpdatedAt", pms)
        } else {
            throw new XError("Неизвестный режим сохранения ('ins', 'upd')")
        }
        //
        return loadReportSource(own)
    }

    @DaoMethod
    Store reportTaskLog(Map<String, Object> params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelTyp", "RT_ParamsComponent", "")
        long reltypParamsComponent = map.get("RT_ParamsComponent")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_TaskLog", "")

        String whe
        String wheV6 = ""
        if (params.containsKey("id"))
            whe = "o.id=${UtCnv.toLong(params.get("id"))}"
        else {
            whe = "o.cls = ${map.get("Cls_TaskLog")}"
            //
            long pt = UtCnv.toLong(params.get("periodType"))
            String dte = UtCnv.toString(params.get("date"))
            UtPeriod utPeriod = new UtPeriod()
            XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
            XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
            wheV6 = "and v6.dateTimeVal between '${d1}' and '${d2}'"
        }

        Map<String, Long> mapFV = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "", "FV_%")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        map.put("FV_Plan", mapFV.get("FV_Plan"))
        map.put("FV_Fact", mapFV.get("FV_Fact"))

        Store st = loadSqlService("""
            select o.id,
                v1.obj as objWorkPlan,
                v2.obj as objTask, null as fullNameTask,
                v4.numberVal as ValuePlan,
                v5.numberVal as ValueFact,
                v6.dateTimeVal as PlanDateStart,
                v7.dateTimeVal as PlanDateEnd,
                v8.dateTimeVal as FactDateStart,
                v9.dateTimeVal as FactDateEnd,
                v12.obj as objLocationClsSection, null as nameLocationClsSection,
                null as plan_objWork, null as plan_fullNameWork, null as plan_objObject, null as plan_fullNameObject,
                null as plan_StartKm, null as plan_FinishKm, null as plan_StartPicket, null as plan_FinishPicket,
                null as plan_PlanDateEnd, null as plan_FactDateEnd,
                null as plan_objSection, null as plan_nameSection, null as plan_objIncident, null as objEvent,
                null as nameEvent, null as objParameterLog, null as objFault,
                null as pvStatus, null as fvStatus, null as nameStatus,
                null as pvCriticality, null as fvCriticality, null as nameCriticality,
                null as StartLink, null as FinishLink, null as RegistrationDateTime, null as AssignDateTime,
                null as objInspection, null as par_CreationDateTime, null as def_CreationDateTime,
                null as relobjComponentParams, null as nameComponentParams,
                null as objDefect, null as nameDefect,
                null as objComponent, null as nameComponent,
                null as ParamsLimit, null as ParamsLimitMax, null as ParamsLimitMin,
                null as ins_objWorkPlan, null as ins_FactDateEnd,
                null as ins_plan_objWork, null as ins_plan_fullNameWork,
                null as ins_plan_PlanDateEnd, null as ins_plan_FactDateEnd, 
                null as ins_plan_objIncident
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_WorkPlan")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Task")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_Value")} and d4.status=${map.get("FV_Plan")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_Value")} and d5.status=${map.get("FV_Fact")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_PlanDateStart")}
                inner join DataPropVal v6 on d6.id=v6.dataprop ${wheV6}
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_PlanDateEnd")}
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_FactDateStart")}
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v9 on d9.id=v9.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=${map.get("Prop_LocationClsSection")}
                left join DataPropVal v12 on d12.id=v12.dataprop
            where ${whe}
        """, "", "repairdata")

        //... Пересечение
        Set<Object> idsTask = st.getUniqueValues("objTask")
        Store stTask = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTask.join(",")})
        """, "", "nsidata")
        StoreIndex indTask = stTask.getIndex("id")
        //
        Set<Object> idsLocation = st.getUniqueValues("objLocationClsSection")
        Store stLocation = loadSqlService("""
            select o.id, v.name
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsLocation.join(",")})
        """, "", "orgstructuredata")
        StoreIndex indLocation = stLocation.getIndex("id")
        //
        Set<Object> idsWorkPlan = st.getUniqueValues("objWorkPlan")
        map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Store stWP = loadSqlService("""
            select o.id,
                v1.obj as objWork, null as fullNameWork,
                v2.obj as objObject, null as fullNameObject,
                v3.numberVal as StartKm,
                v4.numberVal as FinishKm,
                v5.numberVal as StartPicket,
                v6.numberVal as FinishPicket,
                v7.dateTimeVal as PlanDateEnd,
                v8.dateTimeVal as FactDateEnd,
                v9.obj as objIncident,
                null as objSection, null as nameSection
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Work")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                left join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_PlanDateEnd")}
                left join DataPropVal v7 on d7.id=v7.dataProp
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v8 on d8.id=v8.dataProp
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=${map.get("Prop_Incident")}
                left join DataPropVal v9 on d9.id=v9.dataProp          
            where o.id in (0${idsWorkPlan.join(",")})
        """, "", "plandata")
        //
        Set<Object> idsWork = stWP.getUniqueValues("objWork")
        Store stWork = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        StoreIndex indWork = stWork.getIndex("id")
        //
        Set<Object> idsObject = stWP.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, ov.fullName, v1.obj as objSection, ov1.name as nameSection
            from Obj o
                left join ObjVer ov on o.id=ov.ownerVer and ov.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on v1.obj=ov1.ownerVer and ov1.lastVer=1
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObject = stObject.getIndex("id")
        //
        for (StoreRecord r in stWP) {
            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null)
                r.set("fullNameWork", rWork.getString("fullName"))
            StoreRecord rObject = indObject.get(r.getLong("objObject"))
            if (rObject != null)
                r.set("fullNameObject", rObject.getString("fullName"))
                r.set("objSection", rObject.getLong("objSection"))
                r.set("nameSection", rObject.getString("nameSection"))
        }
        StoreIndex indWP = stWP.getIndex("id")
        //
        for (StoreRecord r in st) {
            StoreRecord recTask = indTask.get(r.getLong("objTask"))
            if (recTask != null)
                r.set("fullNameTask", recTask.getString("fullName"))

            StoreRecord rLocation = indLocation.get(r.getLong("objLocationClsSection"))
            if (rLocation != null)
                r.set("nameLocationClsSection", rLocation.getString("name"))

            StoreRecord rWP = indWP.get(r.getLong("objWorkPlan"))
            if (rWP != null) {
                r.set("plan_objWork", rWP.getLong("objWork"))
                r.set("plan_fullNameWork", rWP.getString("fullNameWork"))
                r.set("plan_objObject", rWP.getLong("objObject"))
                r.set("plan_fullNameObject", rWP.getString("fullNameObject"))
                r.set("plan_objSection", rWP.getLong("objSection"))
                r.set("plan_nameSection", rWP.getString("nameSection"))
                r.set("plan_StartKm", rWP.getLong("StartKm"))
                r.set("plan_FinishKm", rWP.getLong("FinishKm"))
                r.set("plan_StartPicket", rWP.getLong("StartPicket"))
                r.set("plan_FinishPicket", rWP.getLong("FinishPicket"))
                r.set("plan_PlanDateEnd", rWP.getString("PlanDateEnd"))
                r.set("plan_FactDateEnd", rWP.getString("FactDateEnd"))
                if (rWP.getLong("objIncident") > 0)
                    r.set("plan_objIncident", rWP.getLong("objIncident"))
            }
        }
        //
        Set<Object> idsIncident = st.getUniqueValues("plan_objIncident")
        Store stIncident = loadSqlService("""
            select o.id,
                v1.obj as objEvent, ov1.name as nameEvent,
                v4.obj as objParameterLog,
                v5.obj as objFault,
                v6.propVal as pvStatus, null as fvStatus, null as nameStatus,
                v7.propVal as pvCriticality, null as fvCriticality, null as nameCriticality,
                v12.numberVal as StartLink,
                v13.numberVal as FinishLink,
                --v14.multiStrVal as Description,
                v17.dateTimeVal as RegistrationDateTime,
                v18.strVal as InfoApplicant,
                v21.dateTimeVal as AssignDateTime,
                null as objInspection, null as par_CreationDateTime, null as relobjComponentParams, 
                null as nameComponentParams, null as ParamsLimit, null as ParamsLimitMax, null as ParamsLimitMin,
                null as objDefect, null as nameDefect, null as objComponent, null as nameComponent, 
                null as def_CreationDateTime, null as ins_objWorkPlan, null as ins_FactDateEnd, null as ins_plan_objWork, 
                null as ins_plan_fullNameWork, null as ins_plan_PlanDateEnd, null as ins_plan_FactDateEnd, 
                null as ins_plan_objIncident
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Event")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_ParameterLog")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_Fault")}
                left join DataPropVal v5 on d5.id=v5.dataprop
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_Status")}
                left join DataPropVal v6 on d6.id=v6.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_Criticality")}
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d12 on d12.objorrelobj=o.id and d12.prop=${map.get("Prop_StartLink")}
                left join DataPropVal v12 on d12.id=v12.dataprop
                left join DataProp d13 on d13.objorrelobj=o.id and d13.prop=${map.get("Prop_FinishLink")}
                left join DataPropVal v13 on d13.id=v13.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=${map.get("Prop_Description")}
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=${map.get("Prop_RegistrationDateTime")}
                left join DataPropVal v17 on d17.id=v17.dataprop
                left join DataProp d18 on d18.objorrelobj=o.id and d18.prop=${map.get("Prop_InfoApplicant")}
                left join DataPropVal v18 on d18.id=v18.dataprop
                left join DataProp d21 on d21.objorrelobj=o.id and d21.prop=${map.get("Prop_AssignDateTime")}
                left join DataPropVal v21 on d21.id=v21.dataprop
            where o.id in (0${idsIncident.join(",")})
        ""","","incidentdata")
        //
        Set<Object> pvsStatus = stIncident.getUniqueValues("pvStatus")
        Store stPV = apiMeta().get(ApiMeta).loadSql("""
            select p.id, p.factorVal, f.name
            from Propval p, Factor f
            where p.id in (0${pvsStatus.join(",")}) and p.factorVal=f.id            
        """, "")
        StoreIndex indStatus = stPV.getIndex("id")
        //
        Set<Object> pvsCriticality = stIncident.getUniqueValues("pvCriticality")
        stPV = apiMeta().get(ApiMeta).loadSql("""
            select p.id, p.factorVal, f.name
            from Propval p, Factor f
            where p.id in (0${pvsCriticality.join(",")}) and p.factorVal=f.id            
        """, "")
        StoreIndex indCriticality = stPV.getIndex("id")
        //
        Set<Object> idsParameterLog = stIncident.getUniqueValues("objParameterLog")
        Store stParameterLog = loadSqlService("""
            select o.id,
                v2.obj as objInspection, 
                v7.dateTimeVal as CreationDateTime,
                v8.relobj as relobjComponentParams, null as nameComponentParams,
                        null as objComponent, null as nameComponent,
                --v14.multiStrVal as Description,
                v15.numberVal as ParamsLimit,
                v16.numberVal as ParamsLimitMax,
                v17.numberVal as ParamsLimitMin
            from Obj o 
                left join ObjVer v on o.id=v.ownerver and v.lastver=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Inspection")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_CreationDateTime")}
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_ComponentParams")}
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=${map.get("Prop_Description")}
                left join DataPropVal v14 on d14.id=v14.dataprop
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=${map.get("Prop_ParamsLimit")}
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=o.id and d16.prop=${map.get("Prop_ParamsLimitMax")}
                left join DataPropVal v16 on d16.id=v16.dataprop
                left join DataProp d17 on d17.objorrelobj=o.id and d17.prop=${map.get("Prop_ParamsLimitMin")}
                left join DataPropVal v17 on d17.id=v17.dataprop
            where o.id in (0${idsParameterLog.join(",")})
        """, "", "inspectiondata")
        //
        Set<Object> idsRelObjComponentParams = stParameterLog.getUniqueValues("relObjComponentParams")
        Store stMemb = apiMeta().get(ApiMeta).loadSql("""
            select id from relclsmember 
            where relcls in (select id from Relcls where reltyp=${reltypParamsComponent})
            order by id
        """, "")
        Store stRO = loadSqlService("""
            select o.id, r1.obj as obj1, ov1.name as name1, r2.obj as obj2, ov2.name as name2
            from Relobj o
                left join relobjmember r1 on o.id = r1.relobj and r1.relclsmember=${stMemb.get(0).getLong("id")}
                left join objver ov1 on ov1.ownerVer=r1.obj and ov1.lastVer=1
                left join relobjmember r2 on o.id = r2.relobj and r2.relclsmember=${stMemb.get(1).getLong("id")}
                left join objver ov2 on ov2.ownerVer=r2.obj and ov2.lastVer=1
            where o.id in (0${idsRelObjComponentParams.join(",")})
        """, "", "nsidata")
        StoreIndex indRO = stRO.getIndex("id")
        for (StoreRecord r in stParameterLog) {
            StoreRecord rec = indRO.get(r.getLong("relobjComponentParams"))
            if (rec != null) {
                r.set("nameComponentParams", rec.getString("name1"))
                r.set("nameComponent", rec.getString("name2"))
                r.set("objComponent", rec.getLong("obj2"))
            }
        }
        StoreIndex indParameterLog = stParameterLog.getIndex("id")
        //
        Set<Object> idsFault = stIncident.getUniqueValues("objFault")
        Store stFault = loadSqlService("""
            select o.id,
                v2.obj as objInspection,
                v7.dateTimeVal as CreationDateTime,
                v8.obj as objDefect, null as nameDefect, null as objComponent, null as nameComponent
                --v14.multiStrVal as Description
            from Obj o
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Inspection")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_CreationDateTime")}
                left join DataPropVal v7 on d7.id=v7.dataprop
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_Defect")}
                left join DataPropVal v8 on d8.id=v8.dataprop
                left join DataProp d14 on d14.objorrelobj=o.id and d14.prop=${map.get("Prop_Description")}
                left join DataPropVal v14 on d14.id=v14.dataprop
            where o.id in (0${idsFault.join(",")})
        """, "", "inspectiondata")
        //
        Set<Object> idsDefect = stFault.getUniqueValues("objDefect")
        Store stDefect = loadSqlService("""
            select o.id, v.name,
                v1.obj as objDefectsComponent, ov1.name as nameDefectsComponent 
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_DefectsComponent")}
                left join DataPropval v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on v1.obj=ov1.ownerVer and ov1.lastVer=1
            where o.id in (0${idsDefect.join(",")})
        """, "", "nsidata")
        StoreIndex indDefect = stDefect.getIndex("id")
        //
        for (StoreRecord r in stFault) {
            StoreRecord rec = indDefect.get(r.getLong("objDefect"))
            if (rec != null) {
                r.set("nameDefect", rec.getString("name"))
                r.set("objComponent", rec.getLong("objDefectsComponent"))
                r.set("nameComponent", rec.getString("nameDefectsComponent"))
            }
        }
        StoreIndex indFault = stFault.getIndex("id")
        //
        for (StoreRecord r in stIncident) {
            StoreRecord rStatus = indStatus.get(r.getLong("pvStatus"))
            if (rStatus != null) {
                r.set("fvStatus", rStatus.getLong("factorVal"))
                r.set("nameStatus", rStatus.getString("name"))
            }
            StoreRecord rCriticality = indCriticality.get(r.getLong("pvCriticality"))
            if (rCriticality != null) {
                r.set("fvCriticality", rCriticality.getLong("factorVal"))
                r.set("nameCriticality", rCriticality.getString("name"))
            }
            StoreRecord rParameterLog = indParameterLog.get(r.getLong("objParameterLog"))
            if (rParameterLog != null) {
                r.set("objInspection", rParameterLog.getLong("objInspection"))
                r.set("par_CreationDateTime", rParameterLog.getString("CreationDateTime"))
                r.set("relobjComponentParams", rParameterLog.getLong("relobjComponentParams"))
                r.set("nameComponentParams", rParameterLog.getString("nameComponentParams"))
                r.set("objComponent", rParameterLog.getLong("objComponent"))
                r.set("nameComponent", rParameterLog.getString("nameComponent"))
                r.set("ParamsLimit", rParameterLog.getDouble("ParamsLimit"))
                r.set("ParamsLimitMax", rParameterLog.getDouble("ParamsLimitMax"))
                r.set("ParamsLimitMin", rParameterLog.getDouble("ParamsLimitMin"))
                //r.set("Description", rParameterLog.getLong("Description"))
            }
            StoreRecord rFault = indFault.get(r.getLong("objFault"))
            if (rFault != null) {
                r.set("objInspection", rFault.getLong("objInspection"))
                r.set("def_CreationDateTime", rFault.getString("CreationDateTime"))
                r.set("objDefect", rFault.getLong("objDefect"))
                r.set("nameDefect", rFault.getString("nameDefect"))
                r.set("objComponent", rFault.getLong("objComponent"))
                r.set("nameComponent", rFault.getString("nameComponent"))
                //r.set("Description", rFault.getLong("Description"))
            }
        }
        //
        Set<Object> idsInspection = stIncident.getUniqueValues("objInspection")
        Store stInspection = loadSqlService("""
            select o.id,
                v1.obj as objWorkPlan,
                v2.dateTimeVal as FactDateEnd,
                null as objWork, null as fullNameWork,
                null as ins_plan_PlanDateEnd, null as ins_plan_FactDateEnd, 
                null as objIncident
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_WorkPlan")}
                left join DataPropval v1 on d1.id=v1.dataProp 
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropval v2 on d2.id=v2.dataProp
            where o.id in (0${idsInspection.join(",")})
        """, "", "inspectiondata")
        //
        idsWorkPlan = stInspection.getUniqueValues("objWorkPlan")
        stWP = loadSqlService("""
            select o.id,
                v1.obj as objWork, null as fullNameWork,
                v7.dateTimeVal as PlanDateEnd,
                v8.dateTimeVal as FactDateEnd,
                v9.obj as objIncident
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Work")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_PlanDateEnd")}
                left join DataPropVal v7 on d7.id=v7.dataProp
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_FactDateEnd")}
                left join DataPropVal v8 on d8.id=v8.dataProp
                left join DataProp d9 on d9.objorrelobj=o.id and d9.prop=${map.get("Prop_Incident")}
                left join DataPropVal v9 on d9.id=v9.dataProp          
            where o.id in (0${idsWorkPlan.join(",")})
        """, "", "plandata")
        //
        idsWork = stWP.getUniqueValues("objWork")
        stWork = loadSqlService("""
            select o.id, v.fullName
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsWork.join(",")})
        """, "", "nsidata")
        indWork = stWork.getIndex("id")
        //
        for (StoreRecord r in stWP) {
            StoreRecord rWork = indWork.get(r.getLong("objWork"))
            if (rWork != null)
                r.set("fullNameWork", rWork.getString("fullName"))
        }
        indWP = stWP.getIndex("id")
        for (StoreRecord r in stInspection) {
            StoreRecord rec = indWP.get(r.getLong("objWorkPlan"))
            if (rec != null) {
                r.set("fullNameWork", rec.getString("fullNameWork"))
                r.set("ins_plan_FactDateEnd", rec.getString("FactDateEnd"))
                r.set("ins_plan_PlanDateEnd", rec.getString("PlanDateEnd"))
                r.set("objWork", rec.getLong("objWork"))
                r.set("objIncident", rec.getLong("objIncident"))
            }
        }
        StoreIndex indInspection = stInspection.getIndex("id")
        //
        for (StoreRecord r in stIncident) {
            StoreRecord rec = indInspection.get(r.getLong("objInspection"))
            if (rec != null) {
                r.set("ins_objWorkPlan", rec.getLong("objWorkPlan"))
                r.set("ins_FactDateEnd", rec.getString("FactDateEnd"))
                r.set("ins_plan_objWork", rec.getLong("objWork"))
                r.set("ins_plan_fullNameWork", rec.getString("fullNameWork"))
                r.set("ins_plan_PlanDateEnd", rec.getString("ins_plan_PlanDateEnd"))
                r.set("ins_plan_FactDateEnd", rec.getString("ins_plan_FactDateEnd"))
                r.set("ins_plan_objIncident", rec.getLong("objIncident"))
            }
        }
        StoreIndex indIncident = stIncident.getIndex("id")
        //...
        for (StoreRecord r in st) {
            StoreRecord rec = indIncident.get(r.getLong("plan_objIncident"))
            if (rec != null) {
                if (rec.getLong("objEvent") > 0)
                    r.set("objEvent", rec.get("objEvent"))
                r.set("nameEvent", rec.getString("nameEvent"))
                if (rec.getLong("objParameterLog") > 0)
                    r.set("objParameterLog", rec.getLong("objParameterLog"))
                if (rec.getLong("objFault") > 0)
                    r.set("objFault", rec.getLong("objFault"))
                if (rec.getLong("pvStatus") > 0)
                    r.set("pvStatus", rec.getLong("pvStatus"))
                if (rec.getLong("pvCriticality") > 0)
                    r.set("pvCriticality", rec.getLong("pvCriticality"))
                if (rec.getLong("StartLink") > 0)
                    r.set("StartLink", rec.getLong("StartLink"))
                if (rec.getLong("FinishLink") > 0)
                    r.set("FinishLink", rec.getLong("FinishLink"))
                r.set("RegistrationDateTime", rec.getString("RegistrationDateTime"))
                r.set("AssignDateTime", rec.getString("AssignDateTime"))
                if (rec.getLong("fvStatus") > 0)
                    r.set("fvStatus", rec.getLong("fvStatus"))
                r.set("nameStatus", rec.getString("nameStatus"))
                if (rec.getLong("fvCriticality") > 0)
                    r.set("fvCriticality", rec.getLong("fvCriticality"))
                r.set("nameCriticality", rec.getString("nameCriticality"))
                if (rec.getLong("objInspection") > 0)
                    r.set("objInspection", rec.getLong("objInspection"))
                r.set("par_CreationDateTime", rec.getString("par_CreationDateTime"))
                if (rec.getLong("relobjComponentParams") > 0)
                    r.set("relobjComponentParams", rec.getLong("relobjComponentParams"))
                r.set("nameComponentParams", rec.getString("nameComponentParams"))
                if (rec.getLong("objComponent") > 0)
                    r.set("objComponent", rec.getLong("objComponent"))
                r.set("nameComponent", rec.getString("nameComponent"))
                if (rec.getDouble("ParamsLimit") > 0)
                    r.set("ParamsLimit", rec.getDouble("ParamsLimit"))
                if (rec.getDouble("ParamsLimitMax") > 0)
                    r.set("ParamsLimitMax", rec.getDouble("ParamsLimitMax"))
                if (rec.getDouble("ParamsLimitMin") > 0)
                    r.set("ParamsLimitMin", rec.getDouble("ParamsLimitMin"))
                r.set("def_CreationDateTime", rec.getString("def_CreationDateTime"))
                if (rec.getLong("objDefect") > 0)
                    r.set("objDefect", rec.getLong("objDefect"))
                r.set("nameDefect", rec.getString("nameDefect"))
                //
                if (rec.getLong("ins_objWorkPlan") > 0)
                    r.set("ins_objWorkPlan", rec.getLong("ins_objWorkPlan"))
                r.set("ins_FactDateEnd", rec.getString("ins_FactDateEnd"))
                if (rec.getLong("ins_plan_objWork") > 0)
                    r.set("ins_plan_objWork", rec.getLong("ins_plan_objWork"))
                r.set("ins_plan_fullNameWork", rec.getString("ins_plan_fullNameWork"))
                r.set("ins_plan_PlanDateEnd", rec.getString("ins_plan_PlanDateEnd"))
                r.set("ins_plan_FactDateEnd", rec.getString("ins_plan_FactDateEnd"))
                if (rec.getLong("ins_plan_objIncident") > 0)
                    r.set("ins_plan_objIncident", rec.getLong("ins_plan_objIncident"))
            }
        }
        //
        return st
    }

    @DaoMethod
    String generateReport(Map<String, Object> params) {
        def dir = new File(mdb.getApp().appdir + File.separator + "reports")
        if (!dir.exists()) {
            dir.mkdirs()
        }
        String id = UUID.randomUUID().toString()
        params.put("fout", id+".xlsx")
        if (UtCnv.toString(params.get("tml")).equalsIgnoreCase("по-4"))
            generateReportPO_4(params)
        else if (UtCnv.toString(params.get("tml")).equalsIgnoreCase("по-6"))
            generateReportPO_6(params)
        else if (UtCnv.toString(params.get("tml")).equalsIgnoreCase("по-1"))
            generateReportPO_1(params)
        else
            throw new XError("Не известный шаблон")
        return id

    }

    void generateReportPO_1(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        String pathtml = mdb.getApp().appdir + File.separator + "tml" + File.separator + "ПО-1.xlsx"
        String pathexel = mdb.getApp().appdir + File.separator + "reports" + File.separator + pms.getString("fout")
        String pathpdf = pathexel.replace(UtFile.ext(pathexel), "pdf")

        // 1. Загрузка исходной книги
        InputStream inputStream = new FileInputStream(pathtml)
        XSSFWorkbook sourceWorkbook = new XSSFWorkbook(inputStream)

        // 2. Создание целевой книги и листа
        XSSFWorkbook targetWorkbook = new XSSFWorkbook()

        XSSFSheet sourceSheet = sourceWorkbook.getSheetAt(0)
        XSSFSheet destSheet = targetWorkbook.createSheet("Лист1")

        // Итерируем по всем столбцам от 0 до последнего используемого
        for (int i = 0; i < 10; i++) {
            // Получаем ширину столбца из исходного листа
            int columnWidth = sourceSheet.getColumnWidth(i)
            // Устанавливаем ту же ширину для соответствующего столбца в целевом листе
            destSheet.setColumnWidth(i, columnWidth)
        }

        RangeCopier copier = new XSSFRangeCopier(sourceSheet, destSheet)
        CellRangeAddress sourceRange = new CellRangeAddress(0, 44, 0, 9)
        CellRangeAddress destRange = new CellRangeAddress(0, 44, 0, 9)
        //
        copier.copyRange(sourceRange, destRange, true, true)
        //
        // Шапка
        String dte = pms.getDate("date").toString(UtDateTime.createFormatter("dd.MM.yyyy"))
        String nameClient = pms.getString("fullNameClient")
        String nameLocation = pms.getString("nameDirectorLocation")

        Row row = destSheet.getRow(0)
        Cell cell = row.getCell(0)
        cell.setCellValue(nameClient)
        row = destSheet.getRow(1)
        cell = row.getCell(0)
        cell.setCellValue(nameLocation)
        // Date
        row = destSheet.getRow(5)
        cell = row.getCell(4)
        cell.setCellValue(dte)
        //
        row = destSheet.getRow(40)
        cell = row.getCell(0)
        cell.setCellValue(pms.getString("nameDirectorPosition"))
        row = destSheet.getRow(41)
        cell = row.getCell(0)
        cell.setCellValue(pms.getString("nameDirectorLocation"))
        cell = row.getCell(8)
        cell.setCellValue(pms.getString("fullNameDirector"))
        //
        String isp = "Исп. " + pms.getString("nameUserPosition").toLowerCase() + " " + pms.getString("fulNameUser")
        String tel = "тел. " + pms.getString("UserPhone")
        row = destSheet.getRow(43)
        cell = row.getCell(0)
        cell.setCellValue(isp)
        row = destSheet.getRow(44)
        cell = row.getCell(0)
        cell.setCellValue(tel)
        //



        //
        try (FileOutputStream fileOut = new FileOutputStream(pathexel)) {
            targetWorkbook.write(fileOut)
        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            try {
                targetWorkbook.close()
            } catch (Exception e) {
                e.printStackTrace()
            }
            Convertor.cnv2pdf(pathexel, pathpdf, false)
        }
    }

    Map<String, Object> loadDataPO_1(Map<String, Object> params) {

        return null
    }

    void generateReportPO_6(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        String pathtml = mdb.getApp().appdir + File.separator + "tml" + File.separator + "ПО-6.xlsx"
        String pathexel = mdb.getApp().appdir + File.separator + "reports" + File.separator + pms.getString("fout")
        String pathpdf = pathexel.replace(UtFile.ext(pathexel), "pdf")
        // 1. Загрузка исходной книги
        InputStream inputStream = new FileInputStream(pathtml)
        XSSFWorkbook sourceWorkbook = new XSSFWorkbook(inputStream)

        // 2. Создание целевой книги и листа
        XSSFWorkbook targetWorkbook = new XSSFWorkbook()

        XSSFSheet sourceSheet = sourceWorkbook.getSheetAt(0)
        XSSFSheet destSheet = targetWorkbook.createSheet("Лист1")

        // Итерируем по всем столбцам от 0 до последнего используемого
        for (int i = 0; i < 10; i++) {
            // Получаем ширину столбца из исходного листа
            int columnWidth = sourceSheet.getColumnWidth(i)
            // Устанавливаем ту же ширину для соответствующего столбца в целевом листе
            destSheet.setColumnWidth(i, columnWidth)
        }

        RangeCopier copier = new XSSFRangeCopier(sourceSheet, destSheet)
        CellRangeAddress sourceRange = new CellRangeAddress(0, 10, 0, 9)
        CellRangeAddress destRange = new CellRangeAddress(0, 10, 0, 9)
        //
        copier.copyRange(sourceRange, destRange, true, true)

        // Шапка
        String dte = pms.getDate("date").toString(UtDateTime.createFormatter("dd.MM.yyyy"))
        String nameClient = pms.getString("fullNameClient")
        String nameLocation = pms.getString("nameDirectorLocation")

        Row row = destSheet.getRow(0)
        Cell cell = row.getCell(0)
        cell.setCellValue(nameClient)
        row = destSheet.getRow(1)
        cell = row.getCell(0)
        cell.setCellValue(nameLocation)
        // Data
        row = destSheet.getRow(5)
        cell = row.getCell(4)
        cell.setCellValue(dte)

        destSheet.getRow(7).setHeightInPoints(3 * destSheet.getDefaultRowHeightInPoints() as float)

        // Данные
        final XSSFCellStyle cellStyle = targetWorkbook.createCellStyle()
        final XSSFCellStyle cellStyleFont = targetWorkbook.createCellStyle()
        final XSSFCellStyle cellStyleFont8 = targetWorkbook.createCellStyle()
        XSSFFont fontBold = targetWorkbook.createFont()
        XSSFFont font8 = targetWorkbook.createFont()
        fontBold.setBold(true)
        cellStyleFont.setFont(fontBold)
        font8.setFontHeightInPoints((short) 8)
        cellStyleFont8.setFont(font8)

        cellStyleFont.setBorderTop(BorderStyle.THIN)
        cellStyleFont.setBorderRight(BorderStyle.THIN)
        cellStyleFont.setBorderBottom(BorderStyle.THIN)
        cellStyleFont.setBorderLeft(BorderStyle.THIN)

        cellStyle.setBorderTop(BorderStyle.THIN)
        cellStyle.setBorderRight(BorderStyle.THIN)
        cellStyle.setBorderBottom(BorderStyle.THIN)
        cellStyle.setBorderLeft(BorderStyle.THIN)
        //
        final XSSFCellStyle cellStyleBold = targetWorkbook.createCellStyle()
        XSSFFont ftBold = targetWorkbook.createFont()
        ftBold.setBold(true)
        cellStyleBold.setFont(ftBold)
        //

        Map<String, Object> mapRes = loadDataPO_6(params)
        Map<String, Long> mapData = mapRes.get("data") as Map<String, Long>
        //
        int rowStart = 11
        int row0 = rowStart
        for (StoreRecord r in mapRes.get("store") as Store) {
            row = destSheet.createRow(row0)
            cell = row.createCell(0)
            cell.setCellStyle(cellStyle)
            cell.setCellValue(r.getString("name"))

            cell = row.createCell(2)
            cell.setCellStyle(cellStyle)
            if (mapData.get("1_"+r.getString("id")) > 0) {
                cell.setCellValue(mapData.get("1_"+r.getString("id")))
            }
            cell = row.createCell(3)
            cell.setCellStyle(cellStyle)
            if (mapData.get("2_"+r.getString("id")) > 0) {
                cell.setCellValue(mapData.get("2_"+r.getString("id")))
            }
            cell = row.createCell(5)
            cell.setCellStyle(cellStyle)
            if (mapData.get("3_"+r.getString("id")) > 0) {
                cell.setCellValue(mapData.get("3_"+r.getString("id")))
            }
            cell = row.createCell(6)
            cell.setCellStyle(cellStyle)
            if (mapData.get("4_"+r.getString("id")) > 0) {
                cell.setCellValue(mapData.get("4_"+r.getString("id")))
            }
            cell = row.createCell(8)
            cell.setCellStyle(cellStyle)
            if (mapData.get("5_"+r.getString("id")) > 0) {
                cell.setCellValue(mapData.get("5_"+r.getString("id")))
            }
            cell = row.createCell(9)
            cell.setCellStyle(cellStyle)
            if (mapData.get("6_"+r.getString("id")) > 0) {
                cell.setCellValue(mapData.get("6_"+r.getString("id")))
            }
            // Sum
            cell = row.createCell(1)
            String formula = String.format("SUM(C%d:D%d)", row0+1, row0+1)
            cell.setCellFormula(formula)
            cell.setCellStyle(cellStyle)
            //
            cell = row.createCell(4)
            formula = String.format("SUM(F%d:G%d)", row0+1, row0+1)
            cell.setCellStyle(cellStyle)
            cell.setCellFormula(formula)
            //
            cell = row.createCell(7)
            formula = String.format("SUM(I%d:J%d)", row0+1, row0+1)
            cell.setCellFormula(formula)
            cell.setCellStyle(cellStyle)
            //
            row0++
        }
        int rowEnd = row0
        // All sum
        row = destSheet.createRow(rowEnd)
        int col = 0
        cell = row.createCell(col++)
        cell.setCellValue("Всего по станционным путям")
        cell.setCellStyle(cellStyle)
        cell.setCellStyle(cellStyleFont)
        //
        int count = UtCnv.toInt((rowEnd - rowStart - 1) / 2)

        for (String c in ['B','C','D','E','F','G','H','I','J']) {
            List<String> lstFormula = new ArrayList<>()
            for (int k=0; k< count+1; k++) {
                int h = rowStart+1 + 2*k
                lstFormula.add(k, String.format("%s%d", c, h))
            }
            cell = row.createCell(col)
            String formula = String.format("SUM(%s)", lstFormula.join(","))
            cell.setCellFormula(formula)
            cell.setCellStyle(cellStyle)
            cell.setCellStyle(cellStyleFont)
            col++
        }
        //
        row = destSheet.createRow(rowEnd+1)
        col = 0
        cell = row.createCell(col++)
        cell.setCellValue("Всего")
        cell.setCellStyle(cellStyle)
        cell.setCellStyle(cellStyleFont)
        for (String c in ['B','C','D','E','F','G','H','I','J']) {
            cell = row.createCell(col)
            String formula = String.format("SUM(%s%d:%s%d)", c, rowStart+1, c, rowEnd)
            cell.setCellFormula(formula)
            cell.setCellStyle(cellStyle)
            cell.setCellStyle(cellStyleFont)
            col++
        }
        //
        row = destSheet.createRow(rowEnd+3)
        cell = row.createCell(0)
        cell.setCellValue(pms.getString("nameDirectorPosition"))
        cell.setCellStyle(cellStyleBold)
        row = destSheet.createRow(rowEnd+4)
        cell = row.createCell(0)
        cell.setCellValue(pms.getString("nameDirectorLocation"))
        cell.setCellStyle(cellStyleBold)
        cell = row.createCell(8)
        cell.setCellValue(pms.getString("fullNameDirector"))
        cell.setCellStyle(cellStyleBold)
        //
        String isp = "Исп. " + pms.getString("nameUserPosition").toLowerCase() + " " + pms.getString("fulNameUser") +
                " тел. " + pms.getString("UserPhone")
        //String tel = "тел. " + pms.getString("UserPhone")
        row = destSheet.createRow(rowEnd+6)
        cell = row.createCell(0)
        cell.setCellValue(isp)
        cell.setCellStyle(cellStyleFont8)
        //row = destSheet.createRow(rowEnd+7)
        //cell = row.createCell(0)
        //cell.setCellValue(tel)
        //cell.setCellStyle(cellStyleFont8)
        //
        try (FileOutputStream fileOut = new FileOutputStream(pathexel)) {
            targetWorkbook.write(fileOut)
        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            try {
                targetWorkbook.close()
            } catch (Exception e) {
                e.printStackTrace()
            }
            Convertor.cnv2pdf(pathexel, pathpdf, true)
        }

    }

    Map<String, Object> loadDataPO_6(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        long objClient = pms.getLong("objClient")
        long objLocation = pms.getLong("objLocation")
        String dte = pms.getString("date")
        if (objClient == 0)
            throw new XError("[Client] не указан")
        if (objLocation == 0)
            throw new XError("[Location] не указан")
        if (dte.isEmpty())
            throw new XError("[Date] не указан")

        Map<String, Object> mapRes = new HashMap<>()

        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Map<String, Long> mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_Section", "")
        Store stRow = loadSqlService("""
            select o.id, o.cls, v.name
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_StartKm")}
                left join DataPropVal v2 on d2.id=v2.dataprop
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_FinishKm")}
                left join DataPropVal v3 on d3.id=v3.dataprop
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_StartPicket")}
                left join DataPropVal v4 on d4.id=v4.dataprop
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_FinishPicket")}
                left join DataPropVal v5 on d5.id=v5.dataprop
            where v.objParent in (
                select o.id
                from Obj o
                    left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Client")}
                    inner join DataPropVal v1 on d1.id=v1.dataProp and v1.obj=${objClient}
                where o.cls=${mapCls.get("Cls_Section")}
            )
            order by v2.numberVal, v4.numberVal, v3.numberVal, v5.numberVal
        """, "", "objectdata")
        mapRes.put("store", stRow)
        if (stRow.size() == 0) {
            mapRes.put("data", new HashMap<Store, Long>())
            return mapRes
        }
        //
        Set<Object> idsRow = stRow.getUniqueValues("id")
        //
        //mdb.outTable(stRow)
        //
        mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("RelCls", "RC_ParamsComponent", "")
        Store stParams = loadSqlService("""
            select o.id, left(v.name, -10) as name
            from Relobj o, RelobjVer v
            where o.id=v.ownerVer and v.lastVer=1 and o.relcls=${mapCls.get("RC_ParamsComponent")}
                and v.name like '%лежащих в пути%' and v.name like '%шпал%'
        """, "", "nsidata")
        if (stParams.size() == 0)
            throw new XError("[Parameter] не найден")
        Store stParams1 = mdb.createStore("IdName")
        Store stParams2 = mdb.createStore("IdName")
        for (StoreRecord r in stParams) {
            if (r.getString("name").contains("подряд"))
                stParams2.add(r)
            else
                stParams1.add(r)
        }
        //
        StoreIndex indParams = stParams.getIndex("id")
        Set<Object> idsParams1 = stParams1.getUniqueValues("id")
        Set<Object> idsParams2 = stParams2.getUniqueValues("id")
        //
        mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_IncidentParameter", "")
        Map<String, Long> mapPV = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_StatusEliminated", "")
        long pv = apiMeta().get(ApiMeta).idPV("FactorVal", mapPV.get("FV_StatusEliminated"), "Prop_Status")
        Store stIncident = loadSqlService("""
            select o.id, 
                v1.obj as objParameterLog
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_ParameterLog")}
                left join DataPropVal v1 on d1.id=v1.dataprop
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Status")}
                inner join DataPropVal v2 on d2.id=v2.dataprop and v2.propVal<>${pv}
            where o.cls=${mapCls.get("Cls_IncidentParameter")}
        """, "", "incidentdata")
        if (stIncident.size() == 0) {
            mapRes.put("data", new HashMap<Store, Long>())
            return mapRes
        }
        Set<Object> idsParameterLog = stIncident.getUniqueValues("objParameterLog")
        //
        Set<Object> idsObjLocation = getIdsObjWithChildren(pms.getLong("objLocation"))
        /*-----------------Количество шпал, лежащих в пути-----------------*/
        UtPeriod utPeriod = new UtPeriod()
        XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), 11, 0)
        String wheV1="and v1.obj in (${idsObjLocation.join(',')})",
                wheV8="and v8.relobj in (0${idsParams1.join(',')})"
        mapCls = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "Cls_ParameterLog", "")
        Map<String, Object> mapDte = new HashMap<>()
        mapDte.put("d1", UtCnv.toString(d1))
        mapDte.put("dte", dte)
        String sqlParams = """
            select o.id, 
                v8.relobj as relobjComponentParams, null as nameParams,
                v15.numberVal as ParamsLimit,
                v16.obj as objWorkPlan, null as objSection 
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_LocationClsSection")}
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Inspection")}
                left join DataPropVal v2 on d2.id=v2.dataprop      
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_CreationDateTime")}
                inner join DataPropVal v7 on d7.id=v7.dataprop and v7.dateTimeVal between :d1 and :dte
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_ComponentParams")}
                inner join DataPropVal v8 on d8.id=v8.dataprop ${wheV8}                
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=${map.get("Prop_ParamsLimit")}
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=v2.obj and d16.prop=${map.get("Prop_WorkPlan")}
                left join DataPropVal v16 on d16.id=v16.dataprop                  
            where o.cls = ${mapCls.get("Cls_ParameterLog")}
        """
        //
        Store stParameterLog1 = apiInspectionData().get(ApiInspectionData).loadSqlWithParams(sqlParams, mapDte, "")
        if (stParameterLog1.size() == 0) {
            d1 = UtCnv.toDate(d1.toJavaLocalDate().minusYears(1))
            mapDte.put("d1", UtCnv.toString(d1))
            stParameterLog1 = apiInspectionData().get(ApiInspectionData).loadSqlWithParams(sqlParams, mapDte, "")
            if (stParameterLog1.size() == 0) {
                mapRes.put("data", new HashMap<Store, Long>())
                return mapRes
            }
        }
        /*-----------------Количество, негодных железобетонных шпал-----------------*/
        String wheV7="and v7.dateTimeVal::date between '1800-01-01' and '${dte}'"
        wheV8="and v8.relobj in (0${idsParams2.join(',')})"
        Store stParameterLog2 = loadSqlService("""
            select o.id, 
                v8.relobj as relobjComponentParams, null as nameParams,
                v15.numberVal as ParamsLimit,
                v16.obj as objWorkPlan, null as objSection 
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_LocationClsSection")}
                inner join DataPropVal v1 on d1.id=v1.dataprop ${wheV1}
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Inspection")}
                left join DataPropVal v2 on d2.id=v2.dataprop      
                left join DataProp d7 on d7.objorrelobj=o.id and d7.prop=${map.get("Prop_CreationDateTime")}
                inner join DataPropVal v7 on d7.id=v7.dataprop ${wheV7}
                left join DataProp d8 on d8.objorrelobj=o.id and d8.prop=${map.get("Prop_ComponentParams")}
                inner join DataPropVal v8 on d8.id=v8.dataprop ${wheV8}                
                left join DataProp d15 on d15.objorrelobj=o.id and d15.prop=${map.get("Prop_ParamsLimit")}
                left join DataPropVal v15 on d15.id=v15.dataprop
                left join DataProp d16 on d16.objorrelobj=v2.obj and d16.prop=${map.get("Prop_WorkPlan")}
                left join DataPropVal v16 on d16.id=v16.dataprop                  
            where o.id in (${idsParameterLog.join(',')})
        """, "", "inspectiondata")
        if (stParameterLog2.size() == 0) {
            mapRes.put("data", new HashMap<Store, Long>())
            return mapRes
        }
        //
        stParameterLog2.add(stParameterLog1)
        //
        Set<Object> idsWorkPlan = stParameterLog2.getUniqueValues("objWorkPlan")
        Store stObject = loadSqlService("""
            select o.id, 
                v1.obj as objObject, null as objSection
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Object")}
                left join DataPropVal v1 on d1.id=v1.dataprop
            where o.id in (${idsWorkPlan.join(',')})
        """, "", "plandata")
        Set<Object> idsObject = stObject.getUniqueValues("objObject")
        Store stSection = loadSqlService("""
            select o.id, 
                v1.obj as objSection
            from Obj o 
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                inner join DataPropVal v1 on d1.id=v1.dataprop and v1.obj in (0${idsRow.join(',')})
            where o.id in (${idsObject.join(',')})
        """, "", "objectdata")
        StoreIndex indSection = stSection.getIndex("id")
        for(StoreRecord r in stObject) {
            StoreRecord rec = indSection.get(r.getLong("objObject"))
            if (rec != null)
                r.set("objSection", rec.getLong("objSection"))
        }
        StoreIndex indObject = stObject.getIndex("id")
        for(StoreRecord r in stParameterLog2) {
            StoreRecord rec = indObject.get(r.getLong("objWorkPlan"))
            if (rec != null)
                r.set("objSection", rec.getLong("objSection"))
            rec = indParams.get(r.getLong("relobjComponentParams"))
            if (rec != null)
                r.set("nameParams", rec.getString("name"))
        }
        //
        //mdb.outTable(stParameterLog)
        //
        Map<String, Long> mapData = new HashMap<>()
        for (Object o in idsRow) {
            long row = UtCnv.toLong(o)
            for (StoreRecord r in stParameterLog2) {
                if (row != r.getLong("objSection"))
                    continue

                if (r.getString("nameParams").toLowerCase().contains("количество деревянных шпал, лежащих в пути"))
                    mapData.put("1_"+r.getString("objSection"), UtCnv.toLong(mapData.get("1_"+r.getString("objSection"))) + r.getLong("paramsLimit"))
                if (r.getString("nameParams").toLowerCase().contains("количество железобетонных шпал, лежащих в пути"))
                    mapData.put("2_"+r.getString("objSection"), UtCnv.toLong(mapData.get("2_"+r.getString("objSection"))) + r.getLong("paramsLimit"))
                if (r.getString("nameParams").toLowerCase().contains("количество, неподряд лежащих в пути негодных деревянных шпал"))
                    mapData.put("3_"+r.getString("objSection"), UtCnv.toLong(mapData.get("3_"+r.getString("objSection"))) + r.getLong("paramsLimit"))
                if (r.getString("nameParams").toLowerCase().contains("количество, неподряд лежащих в пути негодных железобетонных шпал"))
                    mapData.put("4_"+r.getString("objSection"), UtCnv.toLong(mapData.get("4_"+r.getString("objSection"))) + r.getLong("paramsLimit"))
                if (r.getString("nameParams").toLowerCase().contains("количество, подряд лежащих в пути негодных деревянных шпал")) {
                    mapData.put("3_" + r.getString("objSection"), UtCnv.toLong(mapData.get("3_" + r.getString("objSection"))) + r.getLong("paramsLimit"))
                    if (r.getLong("paramsLimit") > 2)
                        mapData.put("5_" + r.getString("objSection"), UtCnv.toLong(mapData.get("5_" + r.getString("objSection"))) + 1)
                }
                if (r.getString("nameParams").toLowerCase().contains("количество, подряд лежащих в пути негодных железобетонных шпал")) {
                    mapData.put("4_"+r.getString("objSection"), UtCnv.toLong(mapData.get("4_"+r.getString("objSection"))) + r.getLong("paramsLimit"))
                    if (r.getLong("paramsLimit") > 2)
                        mapData.put("6_" + r.getString("objSection"), UtCnv.toLong(mapData.get("6_" + r.getString("objSection"))) + 1)
                }
            }
        }

        //mdb.outMap(mapData)
        mapRes.put("data", mapData)

        return mapRes
    }

    void generateReportPO_4(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        String pathtml = mdb.getApp().appdir + File.separator + "tml" + File.separator + "ПО-4.xlsx"
        String pathexel = mdb.getApp().appdir + File.separator + "reports" + File.separator + pms.getString("fout")
        String pathpdf = pathexel.replace(UtFile.ext(pathexel), "pdf")

        // 1. Загрузка исходной книги
        InputStream inputStream = new FileInputStream(pathtml)
        XSSFWorkbook sourceWorkbook = new XSSFWorkbook(inputStream)

        // 2. Создание целевой книги и листа
        XSSFWorkbook targetWorkbook = new XSSFWorkbook()

        XSSFSheet sourceSheet = sourceWorkbook.getSheetAt(0)
        XSSFSheet destSheet = targetWorkbook.createSheet("Лист1")

        // Итерируем по всем столбцам от 0 до последнего используемого
        for (int i = 0; i < 10; i++) {
            // Получаем ширину столбца из исходного листа
            int columnWidth = sourceSheet.getColumnWidth(i)
            // Устанавливаем ту же ширину для соответствующего столбца в целевом листе
            destSheet.setColumnWidth(i, columnWidth)
        }

        RangeCopier copier = new XSSFRangeCopier(sourceSheet, destSheet)
        CellRangeAddress sourceRange = new CellRangeAddress(0, 71, 0, 9)
        CellRangeAddress destRange = new CellRangeAddress(0, 71, 0, 9)
        //
        copier.copyRange(sourceRange, destRange, true, true)
        //
        long pt = pms.getLong("periodType")
        String dte = pms.getString("date")
        UtPeriod utPeriod = new UtPeriod()
        XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
        XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
        PeriodGenerator pg = new PeriodGenerator()
        String namePeriod = pg.getPeriodName(d1, d2, pt, 1)
        String h2 = ""
        Store stLocation = loadSqlService("""
            select v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id=${pms.getLong("objLocation")}
        """, "", "orgstructuredata")
        if (stLocation.size() > 0)
            h2 = "за " + namePeriod + " по " + stLocation.get(0).getString("name").toLowerCase()
        //
        Row row = destSheet.getRow(5)
        Cell cell = row.getCell(0)
        cell.setCellValue(h2)
        //
        row = destSheet.getRow(67)
        cell = row.getCell(0)
        cell.setCellValue(pms.getString("nameDirectorPosition"))
        row = destSheet.getRow(68)
        cell = row.getCell(0)
        cell.setCellValue(pms.getString("nameDirectorLocation"))
        cell = row.getCell(8)
        cell.setCellValue(pms.getString("fullNameDirector"))
        //
        String isp = "Исп. " + pms.getString("nameUserPosition").toLowerCase() + " " + pms.getString("fulNameUser")
        String tel = "тел. " + pms.getString("UserPhone")
        row = destSheet.getRow(70)
        cell = row.getCell(0)
        cell.setCellValue(isp)
        row = destSheet.getRow(71)
        cell = row.getCell(0)
        cell.setCellValue(tel)
        //

        // Данные
        Map<String, Map<String, Long>> mapData = loadDataPO_4(params)
        //
        row = destSheet.getRow(63)
        cell = row.getCell(8)
        cell.setCellValue(mapData.get("shtuka").get("shtuka"))

        def rowNums = [13, 14, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 27, 28, 29, 31, 32, 33, 34, 35, 36, 37, 39, 40, 41, 42, 43, 44, 46, 47, 48, 49, 50, 52, 53, 54, 56, 57, 59]
        def rowIndex = ["10", "11", "12", "13", "14", "18", "20", "21", "24", "25", "26", "27", "31", "30", "38", "40", "41", "43", "44", "46", "47", "49", "50", "52", "53", "55", "56", "59", "60", "62", "65", "66", "69", "70", "74", "79", "85", "86", "99"]

        for (def i = 0; i < rowNums.size(); i++) {
            row = destSheet.getRow(rowNums[i])
            cell = row.getCell(4)
            if (mapData.get(rowIndex[i]) && mapData.get(rowIndex[i]).get("75c"))
                cell.setCellValue(mapData.get(rowIndex[i]).get("75c"))
            cell = row.getCell(5)
            if (mapData.get(rowIndex[i]) && mapData.get(rowIndex[i]).get("75z"))
                cell.setCellValue(mapData.get(rowIndex[i]).get("75z"))
            cell = row.getCell(6)
            if (mapData.get(rowIndex[i]) && mapData.get(rowIndex[i]).get("65c"))
                cell.setCellValue(mapData.get(rowIndex[i]).get("65c"))
            cell = row.getCell(7)
            if (mapData.get(rowIndex[i]) && mapData.get(rowIndex[i]).get("65z"))
                cell.setCellValue(mapData.get(rowIndex[i]).get("65z"))
            cell = row.getCell(8)
            if (mapData.get(rowIndex[i]) && mapData.get(rowIndex[i]).get("50"))
                cell.setCellValue(mapData.get(rowIndex[i]).get("50"))
            cell = row.getCell(9)
            if (mapData.get(rowIndex[i]) && mapData.get(rowIndex[i]).get("43"))
                cell.setCellValue(mapData.get(rowIndex[i]).get("43"))
        }

        try (FileOutputStream fileOut = new FileOutputStream(pathexel)) {
            targetWorkbook.write(fileOut)
        } catch (Exception e) {
            e.printStackTrace()
        } finally {
            try {
                targetWorkbook.close()
            } catch (Exception e) {
                e.printStackTrace()
            }
            Convertor.cnv2pdf(pathexel, pathpdf, false)
        }
    }

    @DaoMethod
    Map<String, Map<String, Long>> loadDataPO_4(Map<String, Object> params) {
        Map<String, Long> map = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Prop", "", "Prop_%")
        Map<String, Long> map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Cls", "", "Cls_%")
        map.put("cls", map2.get("Cls_TaskLog"))
        map.put("Cls_RailwayStage", map2.get("Cls_RailwayStage"))
        map.put("Cls_RailwayStation", map2.get("Cls_RailwayStation"))
        map2 = apiMeta().get(ApiMeta).getIdFromCodOfEntity("Factor", "FV_Fact", "")
        map.put("FV_Fact", map2.get("FV_Fact"))
        //
        map.put("objClient", UtCnv.toLong(params.get("objClient")))
        //
        String dte = UtCnv.toString(params.get("date"))
        UtPeriod utPeriod = new UtPeriod()
        long pt = UtCnv.toLong(params.get("periodType"))
        XDate d1 = utPeriod.calcDbeg(UtCnv.toDate(dte), pt, 0)
        XDate d2 = utPeriod.calcDend(UtCnv.toDate(dte), pt, 0)
        Store stTasks = loadSqlService("""
            select o.id
            from Obj o, ObjVer v
            where o.id=v.ownerVer and v.lastVer=1 and 
                (LOWER(v.name) like 'смена рельса р75 с%' or LOWER(v.name) like 'смена рельса р75 з%' or
                LOWER(v.name) like 'смена рельса р65 с%' or LOWER(v.name) like 'смена рельса р65 з%' or
                LOWER(v.name) like 'смена рельса р50%' or LOWER(v.name) like 'смена рельса р43%')
        """, "", "nsidata")
        Set<Object> idsTasks = stTasks.getUniqueValues("id")
        //
        Store st = loadSqlService("""
            select o.id, o.cls, v2.dateTimeVal as FactDateEnd, v3.obj as objTask, v1.numberVal as Value, 
                v4.obj as objWorkPlan, v5.obj as objLocation, v6.obj as objObject, null as nameObject
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Value")} and d1.status=${map.get("FV_Fact")}
                inner join DataPropVal v1 on d1.id=v1.dataProp
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_FactDateEnd")}
                inner join DataPropVal v2 on d2.id=v2.dataProp and v2.dateTimeVal between '${d1}' and '${d2}'
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_Task")}
                inner join DataPropVal v3 on d3.id=v3.dataProp and v3.obj in (0${idsTasks.join(",")})
                left join DataProp d4 on d4.objorrelobj=o.id and d4.prop=${map.get("Prop_WorkPlan")}
                left join DataPropVal v4 on d4.id=v4.dataProp
                left join DataProp d5 on d5.objorrelobj=o.id and d5.prop=${map.get("Prop_LocationClsSection")}
                left join DataPropVal v5 on d5.id=v5.dataProp
                left join DataProp d6 on d6.objorrelobj=o.id and d6.prop=${map.get("Prop_Object")}
                left join DataPropVal v6 on d6.id=v6.dataProp                    
            where o.cls=${map.get("cls")}
        """, "Report.po_4", "repairdata")
        //
        if (st.size() == 0)
            throw new XError("Нет данных")
        //

        Set<Object> idsWorkPlan = st.getUniqueValues("objWorkPlan")
        //
        long objLocation = UtCnv.toLong(params.get("objLocation"))
        Store stLocation = loadSqlService("""
           WITH RECURSIVE r AS (
               SELECT o.id, v.objParent as parent
               FROM Obj o, ObjVer v
               WHERE o.id=v.ownerver and v.lastver=1 and v.objParent=${objLocation}
               UNION ALL
               SELECT t.*
               FROM ( SELECT o.id, v.objParent as parent
                      FROM Obj o, ObjVer v
                      WHERE o.id=v.ownerver and v.lastver=1
                    ) t
                  JOIN r
                      ON t.parent = r.id
           ),
           o as (
           SELECT o.id, v.objParent as parent
           FROM Obj o, ObjVer v
           WHERE o.id=v.ownerver and v.lastver=1 and o.id=${objLocation}
           )
           SELECT * FROM o
           UNION ALL
           SELECT * FROM r
           where 0=0
        """, "", "orgstructuredata")

        Set<Object> idsLocation = stLocation.getUniqueValues("id")
        //

        Store stWorkPlan = loadSqlService("""
            select o.id, o.cls,  
                v1.obj as objLocation, v2.obj as objObject, null as clsObject, null as nameObject, v3.obj as objIncident 
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_LocationClsSection")}
                inner join DataPropVal v1 on d1.id=v1.dataProp and v1.obj in (0${idsLocation.join(",")})
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Object")}
                inner join DataPropVal v2 on d2.id=v2.dataProp
                left join DataProp d3 on d3.objorrelobj=o.id and d3.prop=${map.get("Prop_Incident")}
                inner join DataPropVal v3 on d3.id=v3.dataProp
            where o.id in (0${idsWorkPlan.join(",")})
        """, "", "plandata")

        Set<Object> idsObject = stWorkPlan.getUniqueValues("objObject")
        Store stObject = loadSqlService("""
            select o.id, o.cls, v.name from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        StoreIndex indObject = stObject.getIndex("id")
        for (StoreRecord r in stWorkPlan) {
            StoreRecord rec = indObject.get(r.getLong("objObject"))
            if (rec != null) {
                r.set("clsobject", rec.getLong("cls"))
                r.set("nameobject", rec.getString("name"))
            }
        }
        //
        StoreIndex indWorkPlan = stWorkPlan.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord rec = indWorkPlan.get(r.getLong("objWorkPlan"))
            if (rec != null) {
                r.set("objObject", rec.getLong("objObject"))
                r.set("clsObject", rec.getLong("clsobject"))
                r.set("nameObject", rec.getString("nameobject"))
                r.set("objIncident", rec.getLong("objIncident"))
            }
        }
        //
        stObject = loadSqlService("""
            select o.id, o.cls, v1.obj as objSection, ov1.objParent as parent
            from Obj o
                left join ObjVer v on o.id=v.ownerVer and v.lastVer=1
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Section")}
                left join DataPropVal v1 on d1.id=v1.dataProp
                left join ObjVer ov1 on ov1.ownerVer=v1.obj and ov1.lastVer=1
            where o.id in (0${idsObject.join(",")})
        """, "", "objectdata")
        indObject = stObject.getIndex("id")

        for (StoreRecord r in st) {
            StoreRecord rec = indObject.get(r.getLong("objObject"))
            if (rec != null) {
                r.set("objSection", rec.getLong("objSection"))
                r.set("Parent", rec.getLong("parent"))
            }
        }
        //
        Set<Object> idsParent = stObject.getUniqueValues("parent")
        stObject = loadSqlService("""
            select o.id, v2.obj as objClient
            from Obj o
                left join DataProp d2 on d2.objorrelobj=o.id and d2.prop=${map.get("Prop_Client")}
                inner join DataPropVal v2 on d2.id=v2.dataProp and v2.obj=${map.get("objClient")}
            where o.id in (0${idsParent.join(",")}) 
        """, "", "objectdata")
        indObject = stObject.getIndex("id")
        for (StoreRecord r in st) {
            StoreRecord rec = indObject.get(r.getLong("Parent"))
            if (rec != null) {
                r.set("objClient", rec.getLong("objClient"))
            }
        }
        //
        Set<Object> idsObjClient = st.getUniqueValues("objClient")
        if (!idsObjClient.contains(UtCnv.toLong(params.get("objClient"))))
            throw new XError("Нет данных")


        //
        Set<Object> idsIncident = stWorkPlan.getUniqueValues("objIncident")
        Store stIncident = loadSqlService("""
            select o.id, v1.obj as objFault
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Fault")}
                left join DataPropVal v1 on d1.id=v1.dataProp
            where o.id in (0${idsIncident.join(",")}) 
        """, "", "incidentdata")
        StoreIndex indIncident = stIncident.getIndex("id")
        Set<Object> idsFault = stIncident.getUniqueValues("objFault")
        //
        Store stInspection = loadSqlService("""
            select o.id, v1.obj as objDefect
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_Defect")}
                left join DataPropVal v1 on d1.id=v1.dataProp
            where o.id in (0${idsFault.join(",")}) 
        """, "", "inspectiondata")
        StoreIndex indInspection = stInspection.getIndex("id")
        Set<Object> idsDefect = stInspection.getUniqueValues("objDefect")
        //
        Store stNsi = loadSqlService("""
            select o.id, v1.strVal as DefectsIndex
            from Obj o
                left join DataProp d1 on d1.objorrelobj=o.id and d1.prop=${map.get("Prop_DefectsIndex")}
                left join DataPropVal v1 on d1.id=v1.dataProp
            where o.id in (0${idsDefect.join(",")}) 
        """, "", "nsidata")

        StoreIndex indNsi = stNsi.getIndex("id")
        //
        Set<Object> idsTask = st.getUniqueValues("objTask")
        Store stTask = loadSqlService("""
            select o.id, v.name 
            from Obj o, ObjVer v where o.id=v.ownerVer and v.lastVer=1 and o.id in (0${idsTask.join(",")})
        """, "", "nsidata")
        StoreIndex indTask = stTask.getIndex("id")
        //
        Store stRes = mdb.createStore("Report.po_4")
        for (StoreRecord r in st) {
            StoreRecord recTask = indTask.get(r.getLong("objTask"))
            if (recTask != null)
                r.set("nameTask", recTask.getString("name"))
            //
            StoreRecord recIncident = indIncident.get(r.getLong("objIncident"))
            if (recIncident != null) {
                r.set("objFault", recIncident.getLong("objFault"))
            }
            //
            StoreRecord recInspection = indInspection.get(r.getLong("objFault"))
            if (recInspection != null) {
                r.set("objDefect", recInspection.getLong("objDefect"))
            }
            //
            StoreRecord recNsi = indNsi.get(r.getLong("objDefect"))
            if (recNsi != null) {
                r.set("DefectsIndex", recNsi.getString("DefectsIndex"))
            }
            //
            if (r.getLong("objClient") > 0)
                stRes.add(r)
        }
        //
        Map<String, Map<String, Long>> mapNum = new HashMap<>()
        Map<String, Long> mapType10 = new HashMap<>()
        Map<String, Long> mapType11 = new HashMap<>()
        Map<String, Long> mapType12 = new HashMap<>()
        Map<String, Long> mapType13 = new HashMap<>()
        Map<String, Long> mapType14 = new HashMap<>()
        Map<String, Long> mapType18 = new HashMap<>()
        Map<String, Long> mapType20 = new HashMap<>()
        Map<String, Long> mapType21 = new HashMap<>()
        Map<String, Long> mapType24 = new HashMap<>()
        Map<String, Long> mapType25 = new HashMap<>()
        Map<String, Long> mapType26 = new HashMap<>()
        Map<String, Long> mapType27 = new HashMap<>()
        Map<String, Long> mapType30 = new HashMap<>()
        Map<String, Long> mapType31 = new HashMap<>()
        Map<String, Long> mapType38 = new HashMap<>()
        Map<String, Long> mapType40 = new HashMap<>()
        Map<String, Long> mapType41 = new HashMap<>()
        Map<String, Long> mapType43 = new HashMap<>()
        Map<String, Long> mapType44 = new HashMap<>()
        Map<String, Long> mapType46 = new HashMap<>()
        Map<String, Long> mapType47 = new HashMap<>()
        Map<String, Long> mapType49 = new HashMap<>()
        Map<String, Long> mapType50 = new HashMap<>()
        Map<String, Long> mapType52 = new HashMap<>()
        Map<String, Long> mapType53 = new HashMap<>()
        Map<String, Long> mapType55 = new HashMap<>()
        Map<String, Long> mapType56 = new HashMap<>()
        Map<String, Long> mapType59 = new HashMap<>()
        Map<String, Long> mapType60 = new HashMap<>()
        Map<String, Long> mapType62 = new HashMap<>()
        Map<String, Long> mapType65 = new HashMap<>()
        Map<String, Long> mapType66 = new HashMap<>()
        Map<String, Long> mapType69 = new HashMap<>()
        Map<String, Long> mapType70 = new HashMap<>()
        Map<String, Long> mapType74 = new HashMap<>()
        Map<String, Long> mapType79 = new HashMap<>()
        Map<String, Long> mapType85 = new HashMap<>()
        Map<String, Long> mapType86 = new HashMap<>()
        Map<String, Long> mapType99 = new HashMap<>()

        long shtuka = 0
        //10.
        for (StoreRecord r in stRes) {
            if (defectsIndex(r.getString("DefectsIndex"))) {
                if (r.getString("nameTask").toLowerCase().contains("смена рельса р75 с") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р75 з") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р65 с") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р65 з") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р50") ||
                        r.getString("nameTask").toLowerCase().contains("смена рельса р43")) {

                    if (r.getLong("clsObject") == map.get("Cls_RailwayStage")) {

                        //10.
                        if (r.getString("DefectsIndex").startsWith("10.")) {
                            if (r.getString("nameTask").toLowerCase().contains("смена рельса р75 с"))
                                mapType10.put("75c", mapType10.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("смена рельса р75 з"))
                                mapType10.put("75z", mapType10.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType10.put("65c", mapType10.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType10.put("65z", mapType10.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType10.put("50", mapType10.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType10.put("43", mapType10.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("10", mapType10)
                        }
                        //11.
                        if (r.getString("DefectsIndex").startsWith("11.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType11.put("75c", mapType11.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType11.put("75z", mapType11.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType11.put("65c", mapType11.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType11.put("65z", mapType11.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType11.put("50", mapType11.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType11.put("43", mapType11.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("11", mapType11)
                        }
                        //12.
                        if (r.getString("DefectsIndex").startsWith("12.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType12.put("75c", mapType12.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType12.put("75z", mapType12.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType12.put("65c", mapType12.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType12.put("65z", mapType12.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType12.put("50", mapType12.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType12.put("43", mapType12.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("12", mapType12)
                        }
                        //13.
                        if (r.getString("DefectsIndex").startsWith("13.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType13.put("75c", mapType13.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType13.put("75z", mapType13.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType13.put("65c", mapType13.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType13.put("65z", mapType13.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType13.put("50", mapType13.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType13.put("43", mapType13.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("13", mapType13)
                        }
                        //14.
                        if (r.getString("DefectsIndex").startsWith("14.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType14.put("75c", mapType14.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType14.put("75z", mapType14.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType14.put("65c", mapType14.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType14.put("65z", mapType14.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType14.put("50", mapType14.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType14.put("43", mapType14.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("14", mapType14)
                        }
                        //18.
                        if (r.getString("DefectsIndex").startsWith("18.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType18.put("75c", mapType18.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType18.put("75z", mapType18.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType18.put("65c", mapType18.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType18.put("65z", mapType18.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType18.put("50", mapType18.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType18.put("43", mapType18.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("18", mapType18)
                        }
                        //20.
                        if (r.getString("DefectsIndex").startsWith("20.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType20.put("75c", mapType20.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType20.put("75z", mapType20.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType20.put("65c", mapType20.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType20.put("65z", mapType20.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType20.put("50", mapType20.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType20.put("43", mapType20.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("20", mapType20)
                        }
                        //21.
                        if (r.getString("DefectsIndex").startsWith("21.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType21.put("75c", mapType21.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType21.put("75z", mapType21.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType21.put("65c", mapType21.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType21.put("65z", mapType21.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType21.put("50", mapType21.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType21.put("43", mapType21.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("21", mapType21)
                        }
                        //24.
                        if (r.getString("DefectsIndex").startsWith("24.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType24.put("75c", mapType24.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType24.put("75z", mapType24.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType24.put("65c", mapType24.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType24.put("65z", mapType24.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType24.put("50", mapType24.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType24.put("43", mapType24.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("24", mapType24)
                        }
                        //25.
                        if (r.getString("DefectsIndex").startsWith("25.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType25.put("75c", mapType25.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType25.put("75z", mapType25.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType25.put("65c", mapType25.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType25.put("65z", mapType25.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType25.put("50", mapType25.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType25.put("43", mapType25.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("25", mapType25)
                        }
                        //26.
                        if (r.getString("DefectsIndex").startsWith("26.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType26.put("75c", mapType26.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType26.put("75z", mapType26.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType26.put("65c", mapType26.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType26.put("65z", mapType26.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType26.put("50", mapType26.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType26.put("43", mapType26.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("26", mapType26)
                        }
                        //27.
                        if (r.getString("DefectsIndex").startsWith("27.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType27.put("75c", mapType27.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType27.put("75z", mapType27.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType27.put("65c", mapType27.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType27.put("65z", mapType27.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType27.put("50", mapType27.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType27.put("43", mapType27.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("27", mapType27)
                        }
                        //30.
                        if (r.getString("DefectsIndex").startsWith("30.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType30.put("75c", mapType30.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType30.put("75z", mapType30.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType30.put("65c", mapType30.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType30.put("65z", mapType30.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType30.put("50", mapType30.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType30.put("43", mapType30.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("30", mapType30)
                        }
                        //31.
                        if (r.getString("DefectsIndex").startsWith("31.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType31.put("75c", mapType31.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType31.put("75z", mapType31.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType31.put("65c", mapType31.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType31.put("65z", mapType31.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType31.put("50", mapType31.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType31.put("43", mapType31.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("31", mapType31)
                        }
                        //38.
                        if (r.getString("DefectsIndex").startsWith("38.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType38.put("75c", mapType38.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType38.put("75z", mapType38.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType38.put("65c", mapType38.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType38.put("65z", mapType38.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType38.put("50", mapType38.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType38.put("43", mapType38.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("38", mapType38)
                        }
                        //40.
                        if (r.getString("DefectsIndex").startsWith("40.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType40.put("75c", mapType40.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType40.put("75z", mapType40.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType40.put("65c", mapType40.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType40.put("65z", mapType40.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType40.put("50", mapType40.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType40.put("43", mapType40.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("40", mapType40)
                        }
                        //41.
                        if (r.getString("DefectsIndex").startsWith("41.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType41.put("75c", mapType41.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType41.put("75z", mapType41.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType41.put("65c", mapType41.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType41.put("65z", mapType41.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType41.put("50", mapType41.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType41.put("43", mapType41.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("41", mapType41)
                        }
                        //43.
                        if (r.getString("DefectsIndex").startsWith("43.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType43.put("75c", mapType43.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType43.put("75z", mapType43.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType43.put("65c", mapType43.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType43.put("65z", mapType43.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType43.put("50", mapType43.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType43.put("43", mapType43.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("43", mapType43)
                        }
                        //44.
                        if (r.getString("DefectsIndex").startsWith("44.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType44.put("75c", mapType44.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType44.put("75z", mapType44.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType44.put("65c", mapType44.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType44.put("65z", mapType44.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType44.put("50", mapType44.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType44.put("43", mapType44.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("44", mapType44)
                        }
                        //46.
                        if (r.getString("DefectsIndex").startsWith("46.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType46.put("75c", mapType46.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType46.put("75z", mapType46.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType46.put("65c", mapType46.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType46.put("65z", mapType46.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType46.put("50", mapType46.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType46.put("43", mapType46.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("46", mapType46)
                        }
                        //47.
                        if (r.getString("DefectsIndex").startsWith("47.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType47.put("75c", mapType47.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType47.put("75z", mapType47.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType47.put("65c", mapType47.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType47.put("65z", mapType47.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType47.put("50", mapType47.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType47.put("43", mapType47.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("47", mapType47)
                        }
                        //49.
                        if (r.getString("DefectsIndex").startsWith("49.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType49.put("75c", mapType49.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType49.put("75z", mapType49.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType49.put("65c", mapType49.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType49.put("65z", mapType49.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType49.put("50", mapType49.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType49.put("43", mapType49.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("49", mapType49)
                        }
                        //50.
                        if (r.getString("DefectsIndex").startsWith("50.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType50.put("75c", mapType50.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType50.put("75z", mapType50.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType50.put("65c", mapType50.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType50.put("65z", mapType50.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType50.put("50", mapType50.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType50.put("43", mapType50.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("50", mapType50)
                        }
                        //52.
                        if (r.getString("DefectsIndex").startsWith("52.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType52.put("75c", mapType52.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType52.put("75z", mapType52.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType52.put("65c", mapType52.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType52.put("65z", mapType52.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType52.put("50", mapType52.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType52.put("43", mapType52.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("52", mapType52)
                        }
                        //53.
                        if (r.getString("DefectsIndex").startsWith("53.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType53.put("75c", mapType53.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType53.put("75z", mapType53.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType53.put("65c", mapType53.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType53.put("65z", mapType53.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType53.put("50", mapType53.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType53.put("43", mapType53.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("53", mapType53)
                        }
                        //55.
                        if (r.getString("DefectsIndex").startsWith("55.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType55.put("75c", mapType55.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType55.put("75z", mapType55.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType55.put("65c", mapType55.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType55.put("65z", mapType55.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType55.put("50", mapType55.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType55.put("43", mapType55.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("55", mapType55)
                        }
                        //56.
                        if (r.getString("DefectsIndex").startsWith("56.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType56.put("75c", mapType56.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType56.put("75z", mapType56.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType56.put("65c", mapType56.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType56.put("65z", mapType56.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType56.put("50", mapType56.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType56.put("43", mapType56.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("56", mapType56)
                        }
                        //59.
                        if (r.getString("DefectsIndex").startsWith("59.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType59.put("75c", mapType59.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType59.put("75z", mapType59.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType59.put("65c", mapType59.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType59.put("65z", mapType59.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType59.put("50", mapType59.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType59.put("43", mapType59.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("59", mapType59)
                        }
                        //60.
                        if (r.getString("DefectsIndex").startsWith("60.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType60.put("75c", mapType60.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType60.put("75z", mapType60.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType60.put("65c", mapType60.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType60.put("65z", mapType60.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType60.put("50", mapType60.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType60.put("43", mapType60.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("60", mapType60)
                        }
                        //62.
                        if (r.getString("DefectsIndex").startsWith("62.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType62.put("75c", mapType62.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType62.put("75z", mapType62.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType62.put("65c", mapType62.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType62.put("65z", mapType62.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType62.put("50", mapType62.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType62.put("43", mapType62.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("62", mapType62)
                        }
                        //65.
                        if (r.getString("DefectsIndex").startsWith("65.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType65.put("75c", mapType65.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType65.put("75z", mapType65.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType65.put("65c", mapType65.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType65.put("65z", mapType65.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType65.put("50", mapType65.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType65.put("43", mapType65.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("65", mapType65)
                        }
                        //66.
                        if (r.getString("DefectsIndex").startsWith("66.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType66.put("75c", mapType66.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType66.put("75z", mapType66.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType66.put("65c", mapType66.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType66.put("65z", mapType66.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType66.put("50", mapType66.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType66.put("43", mapType66.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("66", mapType66)
                        }
                        //69.
                        if (r.getString("DefectsIndex").startsWith("69.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType69.put("75c", mapType69.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType69.put("75z", mapType69.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType69.put("65c", mapType69.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType69.put("65z", mapType69.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType69.put("50", mapType69.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType69.put("43", mapType69.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("69", mapType69)
                        }
                        //70.
                        if (r.getString("DefectsIndex").startsWith("70.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType70.put("75c", mapType70.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType70.put("75z", mapType70.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType70.put("65c", mapType70.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType70.put("65z", mapType70.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType70.put("50", mapType70.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType70.put("43", mapType70.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("70", mapType70)
                        }
                        //74.
                        if (r.getString("DefectsIndex").startsWith("74.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType74.put("75c", mapType74.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType74.put("75z", mapType74.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType74.put("65c", mapType74.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType74.put("65z", mapType74.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType74.put("50", mapType74.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType74.put("43", mapType74.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("74", mapType74)
                        }
                        //79.
                        if (r.getString("DefectsIndex").startsWith("79.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType79.put("75c", mapType79.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType79.put("75z", mapType79.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType79.put("65c", mapType79.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType79.put("65z", mapType79.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType79.put("50", mapType79.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType79.put("43", mapType79.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("79", mapType79)
                        }
                        //85.
                        if (r.getString("DefectsIndex").startsWith("85.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType85.put("75c", mapType85.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType85.put("75z", mapType85.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType85.put("65c", mapType85.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType85.put("65z", mapType85.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType85.put("50", mapType85.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType85.put("43", mapType85.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("85", mapType85)
                        }
                        //86.
                        if (r.getString("DefectsIndex").startsWith("86.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType86.put("75c", mapType86.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType86.put("75z", mapType86.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType86.put("65c", mapType86.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType86.put("65z", mapType86.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType86.put("50", mapType86.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType86.put("43", mapType86.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("86", mapType86)
                        }
                        //99.
                        if (r.getString("DefectsIndex").startsWith("99.")) {
                            if (r.getString("nameTask").toLowerCase().contains("р75 с"))
                                mapType99.put("75c", mapType99.get("75c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р75 з"))
                                mapType99.put("75z", mapType99.get("75z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 с"))
                                mapType99.put("65c", mapType99.get("65c", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р65 з"))
                                mapType99.put("65z", mapType99.get("65z", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р50"))
                                mapType99.put("50", mapType99.get("50", 0L) + r.getLong("Value"))
                            else if (r.getString("nameTask").toLowerCase().contains("р43"))
                                mapType99.put("43", mapType99.get("43", 0L) + r.getLong("Value"))
                            mapNum.put("99", mapType99)
                        }
                    } else if (r.getLong("clsObject") == map.get("Cls_RailwayStation")) {
                        shtuka = shtuka + r.getLong("Value")
                    }
                }
            }
        }

        mapNum.put("shtuka", Map.of("shtuka", shtuka))
        //
        return mapNum
    }

    static boolean defectsIndex(String index) {
        return index.startsWith("10.") || index.startsWith("11.") || index.startsWith("12.") || index.startsWith("13.") ||
                index.startsWith("14.") || index.startsWith("18.") || index.startsWith("20.") || index.startsWith("21.") ||
                index.startsWith("24.") || index.startsWith("25.") || index.startsWith("26.") || index.startsWith("27.") ||
                index.startsWith("30.") || index.startsWith("31.") || index.startsWith("38.") || index.startsWith("40.") ||
                index.startsWith("43.") || index.startsWith("44.") || index.startsWith("46.") || index.startsWith("47.") ||
                index.startsWith("49.") || index.startsWith("50.") || index.startsWith("52.") || index.startsWith("53.") ||
                index.startsWith("55.") || index.startsWith("56.") || index.startsWith("59.") || index.startsWith("60.") ||
                index.startsWith("62.") || index.startsWith("65.") || index.startsWith("66.") || index.startsWith("69.") ||
                index.startsWith("70.") || index.startsWith("74.") || index.startsWith("79.") || index.startsWith("85.") ||
                index.startsWith("86.") || index.startsWith("99.") || index.startsWith("41.")
    }

    private Set<Object> getIdsObjWithChildren(long obj) {
        Store st = loadSqlService("""
           WITH RECURSIVE r AS (
               SELECT o.id, v.objParent as parent
               FROM Obj o, ObjVer v
               WHERE o.id=v.ownerver and v.lastver=1 and v.objParent=${obj}
               UNION ALL
               SELECT t.*
               FROM ( SELECT o.id, v.objParent as parent
                      FROM Obj o, ObjVer v
                      WHERE o.id=v.ownerver and v.lastver=1
                    ) t
                  JOIN r
                      ON t.parent = r.id
           ),
           o as (
           SELECT o.id, v.objParent as parent
           FROM Obj o, ObjVer v
           WHERE o.id=v.ownerver and v.lastver=1 and o.id=${obj}
           )
           SELECT * FROM o
           UNION ALL
           SELECT * FROM r
           where 0=0
        """, "", "orgstructuredata")

        return st.getUniqueValues("id")
    }

    //-------------------------

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
            long fv = UtCnv.toLong(params.get("fvStatus"))
            if (fv == 0)
                fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
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
                long fv = UtCnv.toLong(params.get("fvStatus"))
                if (fv == 0)
                    fv = apiMeta().get(ApiMeta).getDefaultStatus(prop)
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
        //Complex
        if ([FD_PropType_consts.complex].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_MetricsComplex") ||
                    cod.equalsIgnoreCase("Prop_PageContainerComplex")) {
                recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // Attrib str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_URL") ||
                    cod.equalsIgnoreCase("Prop_Method") ||
                    cod.equalsIgnoreCase("Prop_FieldName")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("strVal", UtCnv.toString(params.get(keyValue)))
                    if (UtCnv.toLong(params.get("idComplex")) > 0)
                        recDPV.set("parent", params.get("idComplex"))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib multistr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_MethodBody") ||
                    cod.equalsIgnoreCase("Prop_Method") ||
                    cod.equalsIgnoreCase("Prop_Filter") ||
                    cod.equalsIgnoreCase("Prop_Row") ||
                    cod.equalsIgnoreCase("Prop_Col") ||
                    cod.equalsIgnoreCase("Prop_FilterVal") ||
                    cod.equalsIgnoreCase("Prop_RowVal") ||
                    cod.equalsIgnoreCase("Prop_ColVal")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("multiStrVal", UtCnv.toString(params.get(keyValue)))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib date
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    recDPV.set("dateTimeVal", UtCnv.toString(params.get(keyValue)))
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_MethodTyp") ||
                    cod.equalsIgnoreCase("Prop_RowTotal") ||
                    cod.equalsIgnoreCase("Prop_ColTotal") ||
                    cod.equalsIgnoreCase("Prop_FieldVal")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
                if (UtCnv.toLong(params.get("idComplex")) > 0)
                    recDPV.set("parent", params.get("idComplex"))
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Measure")) {
                if (propVal > 0) {
                    recDPV.set("propVal", propVal)
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }

        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Value")) {
                if (params.get(keyValue) != null || params.get(keyValue) != "") {
                    double v = UtCnv.toDouble(params.get(keyValue))
                    v = v / koef
                    if (digit) v = v.round(digit)
                    recDPV.set("numberval", v)
                    if (UtCnv.toLong(params.get("idComplex")) > 0)
                        recDPV.set("parent", params.get("idComplex"))
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        //Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_User")) {
                if (objRef > 0) {
                    recDPV.set("propVal", propVal)
                    recDPV.set("obj", objRef)
                    if (UtCnv.toLong(params.get("idComplex")) > 0)
                        recDPV.set("parent", params.get("idComplex"))
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

        long au = getUser()
        recDPV.set("authUser", au)
        recDPV.set("inputType", FD_InputType_consts.app)
        long idDPV = mdb.getNextId("DataPropVal")
        recDPV.set("id", idDPV)
        recDPV.set("ord", idDPV)
        recDPV.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME))
        mdb.insertRec("DataPropVal", recDPV, false)
        if ([FD_PropType_consts.complex].contains(propType)) {
            params.put("idComplex", idDPV)
        }
    }

    private void updateProperties(String cod, Map<String, Object> params) {
        VariantMap mapProp = new VariantMap(params)
        String keyValue = cod.split("_")[1]
        long idVal = mapProp.getLong("id" + keyValue)
        long propVal = mapProp.getLong("pv" + keyValue)
        long objRef = mapProp.getLong("obj" + keyValue)
        Store stProp = apiMeta().get(ApiMeta).getPropInfo(cod)
        //
        long propType = stProp.get(0).getLong("propType")
        long attribValType = stProp.get(0).getLong("attribValType")
        Integer digit = null
        double koef = stProp.get(0).getDouble("koef")
        if (koef == 0) koef = 1
        if (!stProp.get(0).isValueNull("digit"))
            digit = stProp.get(0).getInt("digit")

        String sql = ""
        def tmst = XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME)
        def strValue = mapProp.getString(keyValue)
        // Attrib str
        if ([FD_AttribValType_consts.str].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_URL") ||
                    cod.equalsIgnoreCase("Prop_Method") ||
                    cod.equalsIgnoreCase("Prop_FieldName")) {
                if (!mapProp.keySet().contains(keyValue) || strValue.trim() == "") {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                } else {
                    sql = "update DataPropval set strVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib multistr
        if ([FD_AttribValType_consts.multistr].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_MethodBody") ||
                    cod.equalsIgnoreCase("Prop_Method") ||
                    cod.equalsIgnoreCase("Prop_Filter") ||
                    cod.equalsIgnoreCase("Prop_Row") ||
                    cod.equalsIgnoreCase("Prop_Col") ||
                    cod.equalsIgnoreCase("Prop_FilterVal") ||
                    cod.equalsIgnoreCase("Prop_RowVal") ||
                    cod.equalsIgnoreCase("Prop_ColVal")) {
                if (!mapProp.keySet().contains(keyValue) || strValue.trim() == "") {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                } else {
                    sql = "update DataPropval set multiStrVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else {
                throw new XError("for dev: [${cod}] отсутствует в реализации")
            }
        }
        // Attrib date
        if ([FD_AttribValType_consts.dt].contains(attribValType)) {
            if (cod.equalsIgnoreCase("Prop_CreatedAt") ||
                    cod.equalsIgnoreCase("Prop_UpdatedAt")) {
                if (!mapProp.keySet().contains(keyValue) || strValue.trim() == "") {
                    sql = """
                        delete from DataPropVal where id=${idVal};
                        delete from DataProp where id in (
                            select id from DataProp
                            except
                            select dataProp as id from DataPropVal
                        );
                    """
                } else {
                    sql = "update DataPropval set dateTimeVal='${strValue}', timeStamp='${tmst}' where id=${idVal}"
                }
            } else
                throw new XError("for dev: [${cod}] отсутствует в реализации")
        }

        // For FV
        if ([FD_PropType_consts.factor].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_MethodTyp") ||
                    cod.equalsIgnoreCase("Prop_RowTotal") ||
                    cod.equalsIgnoreCase("Prop_ColTotal") ||
                    cod.equalsIgnoreCase("Prop_FieldVal")) {
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

        // For Measure
        if ([FD_PropType_consts.measure].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Measure")) {
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
        // For Meter
        if ([FD_PropType_consts.meter, FD_PropType_consts.rate].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_Value")) {
                if (mapProp[keyValue] != "") {
                    def v = mapProp.getDouble(keyValue)
                    v = v / koef
                    if (digit) v = v.round(digit)
                    sql = "update DataPropval set numberVal=${v}, timeStamp='${tmst}' where id=${idVal}"
                } else {
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
        // For Typ
        if ([FD_PropType_consts.typ].contains(propType)) {
            if (cod.equalsIgnoreCase("Prop_User")) {
                if (objRef > 0)
                    sql = "update DataPropval set propVal=${propVal}, obj=${objRef}, timeStamp='${tmst}' where id=${idVal}"
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

    private void validateForDeleteOwner(long owner) {
        //
        Store stObj = mdb.loadQuery("""
            select v.name
            from Obj o
                inner join ObjVer v on o.id=v.ownerver and v.lastver=1 and v.objparent=${owner}
            where 0=0
        """)
        if (stObj.size() > 0)
            throw new XError("Существуют дочерние объекты ["+ stObj.getUniqueValues("name").join(", ") + "]")
    }
    /**
     *
     * @param id Id Obj
     * Delete object with properties
     */
    @DaoMethod
    void deleteObjWithProperties(long id) {
        //
        validateForDeleteOwner(id)
        //
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "Obj")
        mdb.execQueryNative("""
            delete from DataPropVal
            where dataProp in (select id from DataProp where isobj=1 and objorrelobj=${id});
            delete from DataProp where id in (
                select id from dataprop
                except
                select dataProp as id from DataPropVal
            );
        """)
        //
        eu.deleteEntity(id)
    }


    private Store loadSqlService(String sql, String domain, String model) {
        if (model.equalsIgnoreCase("userdata"))
            return apiUserData().get(ApiUserData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("nsidata"))
            return apiNSIData().get(ApiNSIData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("personnaldata"))
            return apiPersonnalData().get(ApiPersonnalData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("orgstructuredata"))
            return apiOrgStructureData().get(ApiOrgStructureData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("objectdata"))
            return apiObjectData().get(ApiObjectData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("plandata"))
            return apiPlanData().get(ApiPlanData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("inspectiondata"))
            return apiInspectionData().get(ApiInspectionData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("clientdata"))
            return apiClientData().get(ApiClientData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("incidentdata"))
            return apiIncidentData().get(ApiIncidentData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("repairdata"))
            return apiRepairData().get(ApiRepairData).loadSql(sql, domain)
        else if (model.equalsIgnoreCase("resourcedata"))
            return apiResourceData().get(ApiResourceData).loadSql(sql, domain)
        else
            throw new XError("Unknown model [${model}]")
    }

    private long getUser() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        long au = authSvc.getCurrentUser().getAttrs().getLong("id")
        if (au == 0)
            au = 1//throw new XError("notLogined")
        return au
    }


}
