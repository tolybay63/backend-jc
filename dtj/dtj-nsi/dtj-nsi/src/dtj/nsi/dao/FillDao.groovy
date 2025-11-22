package dtj.nsi.dao

import dtj.nsi.dao.utils.XLSXReader_withoutDescription
import jandcode.commons.UtCnv
import jandcode.commons.error.XError
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.domain.Domain
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.dta.ApiNSIData
import tofi.api.dta.ApiObjectData
import tofi.api.mdl.ApiMeta
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService

class FillDao extends BaseMdbUtils {

    ApinatorApi apiMeta() {
        return getApp().bean(ApinatorService.class).getApi("meta")
    }
    ApinatorApi apiNsiData() {
        return app.bean(ApinatorService).getApi("nsidata")
    }
    ApinatorApi apiObjectData() {
        return app.bean(ApinatorService).getApi("objectdata")
    }

    protected static boolean isInteger(String s) {
        if (s == null || s.isEmpty()) return false
        for (int i = 0; i < s.length(); i++) {
            if (!Character.isDigit(s.charAt(i))) {
                return false
            }
        }
        return true
    }

    @DaoMethod
    Store loadLog() {
        return mdb.loadQuery("""
            select * from log
        """)
    }


    @DaoMethod
    void fillObjectService(File file, boolean fill) {
        Store st = mdb.createStore()
        Domain d = mdb.createDomain(st)
        XLSXReader_withoutDescription reader = new XLSXReader_withoutDescription(mdb, file, d, st)
        List<String> fields = reader.getFields()
        //свойства типа атрибут
        def props_atrib = [
                "Prop_Specs": 1089L,
                "Prop_LocationDetails": 1078L,
                "Prop_Number": 1135L,
                "Prop_InstallationDate": 1090
        ]

        //свойства типа измеритель
        def props_meter = [
                "Prop_StartKm": 1007L,
                "Prop_StartPicket": 1009L,
                "Prop_FinishKm": 1008L,
                "Prop_FinishPicket": 1010L,
                "Prop_PeriodicityReplacement": 1131L
        ]

        //свойства типа объект
        def props_obj = [
                "Prop_ObjectType": 1108L,
                "Prop_Section": 1142L
        ]


        //DataDao dao = new DataDao(mdb)
        StoreIndex indexObjectType, indexSection
        //*******************************************************
        // Анализ свойств
        //******************************************************
        def eachProps = { def own, m ->

            /* Obj */
            for (def k : props_obj.keySet()) {
                if (!m.get(k)) continue
                Map<String, Object> pms = new HashMap<>()
                pms.put("own", own)
                pms.put(k, props_obj.get(k))
                pms.put("obj" + k.split("_")[1], m.get(k))
                if (k=="Prop_ObjectType") {
                    StoreRecord rec = indexObjectType.get(UtCnv.toLong(m.get(k)))
                    if (rec != null) {
                        pms.put("pv" + k.split("_")[1], rec.getLong("pv"))
                        apiObjectData().get(ApiObjectData).fillProperties(true, k, pms)
                    }
                } else if (k=="Prop_Section") {
                    StoreRecord rec = indexSection.get(UtCnv.toLong(m.get(k)))
                    if (rec != null) {
                        pms.put("pv" + k.split("_")[1], rec.getLong("pv"))
                        apiObjectData().get(ApiObjectData).fillProperties(true, k, pms)
                    }
                }
            }

            /* Attrib */
            for (def k : props_atrib.keySet()) {
                if (!m.get(k)) continue
                Map<String, Object> pms = new HashMap<>()
                pms.put("own", own)
                pms.put(k, props_atrib.get(k))
                pms.put(k.split("_")[1], m.get(k))
                if (UtCnv.toString(m.get(k)).trim() != "") {
                    apiObjectData().get(ApiObjectData).fillProperties(true, k, pms)
                }
            }

            /* Meter */
            for (def k : props_meter.keySet()) {
                if (!m.get(k)) continue
                Map<String, Object> pms = new HashMap<>()
                pms.put("own", own)
                pms.put(k, props_meter.get(k))
                pms.put(k.split("_")[1], m.get(k))
                if (UtCnv.toDouble(m.get(k)) != 0) {
                    apiObjectData().get(ApiObjectData).fillProperties(true, k, pms)
                }
            }

        }


        //*******************************************************
        // Функция обработки строк файла
        //******************************************************
        def eachLine = { def m ->
            long idObj = 0L
            def cls = UtCnv.toLong(m.get("cls"))
            def name = UtCnv.toString(m.get("name")).trim()
            def fullName = UtCnv.toString(m.get("fullName")).trim()

            if (cls > 0 && name) {

                try {
                    Map<String, Object> params = new HashMap<>()
                    params.put("name", name)
                    params.put("fullName", fullName)
                    params.put("cls", cls)
                    params.put("isObj", 1)
                    params.put("tableName", "Obj")
                    params.put("mode", "ins")
                    idObj = apiObjectData().get(ApiObjectData).createOwner(params)

                } catch (Exception e) {
                    println("Ошибка при создании Obj (cls, name) = ${cls}, ${fullName}")
                    e.printStackTrace()
                }

                if (idObj > 0)
                    eachProps(idObj, m)
            }
        }

        //*******************************************************
        // Функция обработки строк файла
        //******************************************************

        Set<Long> idsCls = new HashSet<>()
        Set<Long> idsObjectType = new HashSet<>()
        Set<Long> idsSection = new HashSet<>()
        def eachLineCalc = { Map m ->
            idsCls.add(UtCnv.toLong(m.get("cls")))
            idsObjectType.add(UtCnv.toLong(m.get("Prop_ObjectType")))
            idsSection.add(UtCnv.toLong(m.get("Prop_Section")))
        }
        //
        Set<String> reqFields = new HashSet<>()
        Set<String> emptyFields = new HashSet<>()
        def count = 0
        def countVal = 0
        if (!fields.contains("cls")) reqFields.add("cls")
        if (!fields.contains("name")) reqFields.add("name")
        if (!fields.contains("fullName")) reqFields.add("fullName")
        if (!fields.contains("Prop_ObjectType")) reqFields.add("Prop_ObjectType")
        if (!fields.contains("Prop_Section")) reqFields.add("Prop_Section")
        if (!fields.contains("Prop_StartKm")) reqFields.add("Prop_StartKm")
        if (!fields.contains("Prop_FinishKm")) reqFields.add("Prop_FinishKm")

        def eachLineTest = { Map m ->
            count++
            if (!m.get("name"))
                emptyFields.add("name: Строка-${count+1}")
            if (!m.get("fullName"))
                emptyFields.add("fullName: Строка-${count+1}")
            if (!isInteger(UtCnv.toString(m.get("cls"))))
                emptyFields.add("cls: Строка-${count+1}")
            if (!isInteger(UtCnv.toString(m.get("Prop_ObjectType"))))
                emptyFields.add("Prop_ObjectType: Строка-${count+1}")
            if (!isInteger(UtCnv.toString(m.get("Prop_Section"))))
                emptyFields.add("Prop_Section: Строка-${count+1}")
            countVal += m.size()
        }

        //*******************************************************
        // Основное тело алгоритма
        //*******************************************************

        if (fill) {
            reader.eachRow(eachLineCalc)
            //
            Store stReg = apiNsiData().get(ApiNSIData).loadSql("""
                select id, cls, 0 as pv from Obj where id in (${idsObjectType.join(",")})
            """, "")
            Map<Long, Long> mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("Cls", "Prop_ObjectType", false)
            for (StoreRecord r in stReg) {
                if (mapPV.get(r.getLong("cls")) > 0)
                    r.set("pv", mapPV.get(r.getLong("cls")))
                else {
                    String msg = "Не указан возможное значение класса [" + r.getString("cls") + "]"
                    mdb.execQuery("""
                        update log set err=1, msg='${msg}', count=${count}, countval=${countVal} where id=1
                    """)
                    throw new XError(msg)
                }
            }
            indexObjectType = stReg.getIndex("id")
            //
            stReg = apiObjectData().get(ApiObjectData).loadSql("""
                select id, cls, 0 as pv from Obj where id in (${idsSection.join(",")})
            """, "")
            mapPV = apiMeta().get(ApiMeta).mapEntityIdFromPV("Cls", "Prop_Section", false)
            for (StoreRecord r in stReg) {
                if (mapPV.get(r.getLong("cls")) > 0)
                    r.set("pv", mapPV.get(r.getLong("cls")))
                else {
                    String msg = "Не указан возможное значение класса [" + r.getString("cls") + "]"
                    mdb.execQuery("""
                        update log set err=1, msg='${msg}', count=${count}, countval=${countVal} where id=1
                    """)
                    throw new XError(msg)
                }
            }
            indexSection = stReg.getIndex("id")
            //
            reader.eachRow(eachLine)
        } else {
            try {
                mdb.execQueryNative("""
                    CREATE TABLE IF NOT EXISTS log (
                        id int8 NOT NULL,
                        msg varchar(800) NULL,
                        count int8 NULL,
                        countval int8 NULL,
                        err int2 NULL,
                        CONSTRAINT pk_log PRIMARY KEY (id)
                    );
                    ALTER TABLE log OWNER TO pg;
                    GRANT ALL ON TABLE log TO pg;
                """)
                Store stLog = mdb.loadQuery("select * from log")
                if (stLog.size()==0) {
                    mdb.execQueryNative("""
                        INSERT INTO log (id, msg, count, countval, err) VALUES (1, '', 0, 0, 0);
                    """)
                } else {
                    mdb.execQueryNative("""
                        UPDATE log SET msg='', count=0, countval=0, err=0 WHERE id=1;
                    """)
                }
            } catch (Exception e) {
                e.printStackTrace()
            }
            //
            reader.eachRow(eachLineTest)
            countVal = countVal - 3*count
            //
            String msg
            def err = 0
            if (!reqFields.isEmpty()) {
                err = 1
                msg = "Наименования полей отсутствуют: [${reqFields.join(', ')}]"
            } else if (!emptyFields.isEmpty()) {
                err = 1
                msg = "Некоторые значения обязательных полей отсутствуют: [${emptyFields.join(', ')}]"
            } else {
                err = 0
                msg = ""
            }
            mdb.execQuery("""
                update log set err='${err}', msg='${msg}', count=${count}, countval=${countVal} where id=1
            """)
        }
    }


}
