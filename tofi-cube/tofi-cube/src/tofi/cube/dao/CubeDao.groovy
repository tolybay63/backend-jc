package tofi.cube.dao

import jandcode.commons.UtDateTime
import jandcode.commons.datetime.XDateTime
import jandcode.commons.datetime.XDateTimeFormatter
import jandcode.commons.error.XError
import jandcode.core.auth.AuthService
import jandcode.core.auth.AuthUser
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.std.CfgService
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.api.mdl.ApiMeta
import tofi.api.mdl.ApiMetaCube
import tofi.api.mdl.model.consts.FD_CubeSActionType_consts
import tofi.api.mdl.model.consts.FD_CubeSDimType_consts
import tofi.api.mdl.model.consts.FD_DimPropType_consts
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService
import tofi.cube.dao.dimsOfCubeS.CubeSDimPeriod

class CubeDao extends BaseMdbUtils {

    ApinatorApi apiMeta() {
        return app.bean(ApinatorService).getApi("meta")
    }

    ApinatorApi apiMetaCube() {
        return app.bean(ApinatorService).getApi("meta")
    }

    def mapError = [isComplete: true, errorMessage: ""]

    @DaoMethod
    Map<String, Object> getCurUserInfo() {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        AuthUser au = authSvc.getCurrentUser()
        if (au == null) {
            throw new XError("NotLogined")
        }
        return au.getAttrs()
    }

    private long getUser() throws Exception {
        AuthService authSvc = mdb.getApp().bean(AuthService.class)
        long au = authSvc.getCurrentUser().getAttrs().getLong("id")
        if (au == 0)
            throw new XError("notLogined")
        return au
    }

    @DaoMethod
    Store loadCubeDb(long cube) {
        Set<Object> setCls = new HashSet<>()
        Set<Object> setRelCls = new HashSet<>()

        Store stTmp = loadSqlMeta("""
            select c.id, d.dimObj, di.typ, di.cls, di.reltyp, di.relCls, di.relTypMember, di.relClsMember
            from Cubes c
                left join CubeSDim d on c.id=d.cubes and d.dimObj is not null
                left join DimObjItem di on d.dimObj=di.dimObj
            where c.id=${cube}
        """, "")

        for (StoreRecord r in stTmp) {
            //cls
            if (!r.isValueNull("cls"))
                setCls.add(r.getLong("cls"))
            //typ
            if (!r.isValueNull("typ")) {
                Store stCls = loadSqlMeta("""
                    select id from Cls where typ=${r.getLong("typ")} 
                """, "")
                setCls.addAll(stCls.getUniqueValues("id"))
            }
            //relCls
            if (!r.isValueNull("relCls"))
                setRelCls.add(r.getLong("relCls"))
            //reltyp
            if (!r.isValueNull("reltyp")) {
                Store stRelCls = loadSqlMeta("""
                    select id from RelCls where reltyp=${r.getLong("reltyp")} 
                """, "")
                setRelCls.addAll(stRelCls.getUniqueValues("id"))
            }
            //relTypMember
            if (!r.isValueNull("relTypMember")) {
                Store stRTM = loadSqlMeta("""
                    select typ, relTypMemb from RelTypMember where id=${r.getLong("relTypMember")} 
                """, "")
                if (!stRTM.get(0).isValueNull("typ")) {
                    Store stCls = loadSqlMeta("""
                        select id from Cls where typ=${stRTM.get(0).getLong("typ")} 
                    """, "")
                    setCls.addAll(stCls.getUniqueValues("id"))
                } else if (!stRTM.get(0).isValueNull("relTypMemb")) {
                    Store stRelCls = loadSqlMeta("""
                        select id from RelCls where reltyp=${stRTM.get(0).getLong("relTypMemb")} 
                    """, "")
                    setRelCls.addAll(stRelCls.getUniqueValues("id"))
                }
            }
            //relClsMember
            if (!r.isValueNull("relClsMember")) {
                Store stRCM = loadSqlMeta("""
                    select cls, relClsMemb from RelClsMember where id=${r.getLong("relClsMember")} 
                """, "")
                if (!stRCM.get(0).isValueNull("cls")) {
                    setCls.addAll(stRCM.get(0).getLong("id"))
                } else if (!stRCM.get(0).isValueNull("relClsMemb")) {
                    Store stRelCls = loadSqlMeta("""
                        select id from RelCls where reltyp=${stRCM.get(0).getLong("relClsMemb")} 
                    """, "")
                    setRelCls.addAll(stRelCls.getUniqueValues("id"))
                }
            }
        }

        Store stTmp1 = loadSqlMeta("""
            select id from CubeSDim
            where cubeS=${cube} and cubeSDimType=${FD_CubeSDimType_consts.obj}
        """, "")

        Store stTmp2 = loadSqlMeta("""
            select c.id from CubeSDim c
            inner join DimProp d on c.dimProp=d.id and d.dimproptype<>${FD_DimPropType_consts.factor}
            where cubeS=${cube} and cubeSDimType=${FD_CubeSDimType_consts.prop}
        """, "")

        if (stTmp1.size() == 0 || stTmp2.size() == 0)
            throw new XError("Куб не настроен")

        if (setCls.isEmpty() && setRelCls.isEmpty())
            throw new XError("Не указана БД")


        return loadSqlMeta("""
            select distinct * from (
                select distinct d.id, d."name"
                from Cls c
                    left join "database" d on c."database" = d.id
                where c.id in (0${setCls.join(",")})
                union all
                select distinct d.id, d."name"
                from RelCls c
                    left join "database" d on c."database" = d.id
                where c.id in (0${setRelCls.join(",")})
            ) a
        """, "")
    }

    @DaoMethod
    Store loadAllCubes() {
        return loadSqlMeta("""
            select -id as id, name, cod, 
                case when parent>0 then -parent else null end as parent, id as ord 
            from CubeSGr
            union all
            select id, name, cod, -cubesgr as parent, ord 
            from cubes
            order by ord
        """, "")
    }
    //////////////////////////

    @DaoMethod
    Map<String, Object> checkCreateCube(long cube) {
        String dOrg = loadSqlMeta("select dOrg from CubeS where id=${cube}",
                "").get(0).getString("dOrg")
        if (dOrg.isEmpty()) {
            throw new XError("Нет даты формирования куба")  //absentDateForm
        }

        Store stDim = loadSqlMeta("""
            select cubesdimtype, dimproptype, dimperiod, dimprop, dimobj 
            from CubeSDim c
            left join DimProp d on c.dimProp=d.id
            where cubeS=${cube}
        """, "")

        Set<Long> doSet = new HashSet<>()
        Set<Long> dpSet = new HashSet<>()
        Set<Long> dpFSet = new HashSet<>()
        Set<Long> dpMLSet = new HashSet<>()
        Set<Long> dpMPSet = new HashSet<>()
        long dPer = 0
        mdb.outTable(stDim)
        for (StoreRecord r : stDim) {
            if (r.getLong("cubeSDimType") == FD_CubeSDimType_consts.period) {
                dPer = r.getLong("dimPeriod")
            } else if (r.getLong("cubeSDimType") == FD_CubeSDimType_consts.obj) {
                doSet.add(r.getLong("dimObj"))
            } else if (r.getLong("cubeSDimType") == FD_CubeSDimType_consts.prop &&
                    r.getLong("dimPropType") == FD_DimPropType_consts.prop) {
                dpSet.add(r.getLong("dimProp"))
            } else if (r.getLong("cubeSDimType") == FD_CubeSDimType_consts.prop &&
                    r.getLong("dimPropType") == FD_DimPropType_consts.factor) {
                dpFSet.add(r.getLong("dimProp"))
            } else if (r.getLong("cubeSDimType") == FD_CubeSDimType_consts.prop &&
                    r.getLong("dimPropType") == FD_DimPropType_consts.multiList) {
                dpMLSet.add(r.getLong("dimProp"))
            } else if (r.getLong("cubeSDimType") == FD_CubeSDimType_consts.prop &&
                    r.getLong("dimPropType") == FD_DimPropType_consts.dimMultiProp) {
                dpMPSet.add(r.getLong("dimProp"))
            }
        }

        /*
            Проверяем наличие свойств зависящих от периода.
            Если такие свойства есть, то исключение
        */
        if (dPer == 0) {
            Store stTmp = loadSqlMeta("""
                    select c.prop, p.cod
                    from CubeSPropCell c
                        left join Prop p on c.prop=p.id
                    where c.cubeS=${cube} and p.isdependvalueonperiod=1
                """, "")
            if (stTmp.size() > 0) {
                Set<Object> setCod = stTmp.getUniqueValues("cod")
                throw new XError("Следующие свойства зависят от периодов: [${setCod.join("; ")}]")
            }
        }


        if (doSet.size() == 0) {
            throw new XError("Нет измерения объектов")
        }

        if (dpSet.size() == 0 && dpMLSet.size() == 0 && dpMPSet.size() == 0) {
            throw new XError("Нет измерения свойств")
        }

        Map<String, Object> mapDimsCube = new HashMap<>()
        mapDimsCube.put("dimObj", doSet)
        mapDimsCube.put("dimProp", dpSet)
        mapDimsCube.put("dimPropF", dpFSet)
        mapDimsCube.put("dimPropML", dpMLSet)
        mapDimsCube.put("dimPropMP", dpMPSet)
        mapDimsCube.put("dimPeriod", dPer)
        //todo
        sleep(1000)
        //
        return mapDimsCube
    }

    private boolean existsCube(long cube) {
        Store st = mdb.loadQuery("""
            select table_name from INFORMATION_SCHEMA.TABLES
            where table_name like 'cubes_${cube}%'
        """)
        return st.size() > 0
    }

    private void removeCube(long cube) {
        XDateTime t1 = XDateTime.now()
        Store st = mdb.loadQuery("""
            select table_name from INFORMATION_SCHEMA.TABLES
            where table_name like 'cubes_${cube}%'
            order by table_name
        """)

        List<String> lstNames = new ArrayList<String>()
        if (st.size() > 0)
            lstNames.add(st.get(0).getString("table_name"))
        for (StoreRecord r : st) {
            if (r.getString("table_name").endsWith("_n"))
                lstNames.add(r.getString("table_name"))
        }
        for (StoreRecord r : st) {
            if (!lstNames.contains(r.getString("table_name")))
                lstNames.add(r.getString("table_name"))
        }

        StringBuilder sb = new StringBuilder()
        for (String nm : lstNames) {
            sb.append("drop table ${nm};")
        }
        mdb.execQuery(sb.toString())
        XDateTime t2 = XDateTime.now()
        addActionInfo([cubeS: cube, cubeSActionType: FD_CubeSActionType_consts.clear,
                       dtBeg: t1, dtEnd: t2, authUser: getUser(), isComplete: true])
    }

    static String getTime(XDateTime dtBeg, XDateTime dtEnd) {
        String res
        double dd = dtEnd.diffMSec(dtBeg) / 1000
        long hh, mm, ssL
        double ss,
               ms = (dd - (long) dd).round(0)

        if (dd < 60) {
            hh = 0
            mm = 0
            ss = dd
            ssL = (long) ss
        } else {
            hh = 0
            mm = (long) (dd / 60)
            ss = (dd % 60 + ms).round(0)
            ssL = (long) ss
            if (mm > 59) {
                hh = (long) (mm / 60)
                mm = mm % 60
            }
        }

        if (hh == 0) {
            if (mm == 0) {
                res = "" + ssL + " сек."
            } else {
                res = "" + mm + " мин. " + ssL + " сек."
            }
        } else {
            res = "" + hh + " ч. " + mm + " мин. " + ssL + " сек."
        }
        //
        return res
    }

    @DaoMethod
    Store loadDataCubeInfo(long cube) {
        Store st = loadSqlMetaWithParams("""
            select t.*, c.cod, fd.text as name, '' as dt
            from DataCubeS t, CubeS c, fd_cubesactiontype fd 
            where t.cubeS=c.id and t.cubeS=:c and t.cubesactiontype=fd.id
            order by t.id desc
        """, "DataCubeS.full", [c: cube])

        for (StoreRecord r in st) {
            String s = getTime(r.getDateTime("dtBeg"), r.getDateTime("dtEnd"))
            r.set("dt", s)
            if (!UtDateTime.isEmpty(r.getDateTime("dtBeg"))) {
                r.set("dtBegStr", r.getDateTime("dtBeg").toString(XDateTimeFormatter.ISO_DATE_TIME));
            }
            if (!UtDateTime.isEmpty(r.getDateTime("dtBeg"))) {
                r.set("dtEndStr", r.getDateTime("dtEnd").toString(XDateTimeFormatter.ISO_DATE_TIME));
            }
        }

        //mdb.outTable(st)

        return st
    }

    //todo
    void addActionInfo(Map<String, Object> params) {
        saveCubeInfo(params)
    }

    @DaoMethod
    void createDimPeriod(long cube, long dimPeriod) {
        if (existsCube(cube))
            removeCube(cube)

        CubeSDimPeriod cdp = new CubeSDimPeriod(mdb, cube, dimPeriod)
        try {
            cdp.createTable()
            cdp.fillTable()
        } catch (Exception e) {
            e.printStackTrace()
            mapError["isComplete"] = false
            mapError["errorMessage"] = e.message
        }


        //todo
        sleep(1000)
        //
    }

    @DaoMethod
    void createDimProp(long cube, Map<String, Set<Long>> mapDP) {
        /* mapDP: dimProp, dimPropF, dimPropML, dimPropMP */
        println("\n")
        println("\n")
        println("DimProp:")
        println(mapDP["dimProp"])
        println("DimPropF:")
        println(mapDP["dimPropF"])
        println("DimPropML:")
        println(mapDP["dimPropML"])
        println("DimPropMP:")
        println(mapDP["dimPropMP"])


        int o = 0

        sleep(1000)

    }

    @DaoMethod
    void createDimObj(long cube, Map<String, Set<Long>> mapDO) {

        int o = 0

        sleep(1000)

    }

    @DaoMethod
    void createFactTable(long cube, Map<String, Object> params) {

        int o = 0

        sleep(1000)

    }


    /**
     Пересечение с данными
     **/
    private Store getData(String modelname) {
        CfgService cfgSvc = getApp().bean(CfgService.class)
        String db = cfgSvc.getConf().getString("dbsource/${modelname}/database")
        return mdb.loadQueryNative("""
            SELECT d.isobj,
                d.own,
                d.prop,
                d.status,
                d.provider,
                d.periodtype,
                d.dbeg,
                d.dend,
                d.iddatapropval,
                d.parentdatapropval,
                d.numberval,
                d.strval,
                d.multistrval,
                d.datetimeval,
                d.propval,
                d.obj,
                d.relobj,
                d.name,
                d.fileval,
                d."timestamp"
               FROM dblink('host=localhost port=5432 user=pg password=1q2w3e4R dbname=${db}'::text, '
            select
            d.isObj,
            d.objorrelobj AS own,
            d.prop,
            coalesce(d.status, 0) status,
            coalesce(d.provider, 0) provider,
            coalesce(d.periodType, 0) periodType,
            v.dbeg,
            v.dend,
            v.id as idDataPropVal,
            v.parent AS parentDataPropVal,
            v.numberval,
            v.strval,
            v.multiStrval,
            v.datetimeval,
            coalesce(v.propVal, 0) propval,
            coalesce(v.obj, 0) obj,
            coalesce(v.relobj, 0) relobj,
            case when d.isobj=1 then obv.name else rov.name end as name,
            v.fileval,
            v."timestamp"
            from dataprop d
            inner join datapropval v on v.dataprop = d.id
            left join ObjVer obv ON d.isobj=1 and v.obj = obv.ownerVer AND obv.lastVer = 1
            left join RelObjVer rov ON d.isobj=0 and v.relobj = rov.ownerVer AND rov.lastVer = 1'::text) 
                d(isobj integer, own bigint, prop bigint, status bigint, provider bigint, periodtype bigint,
                dbeg date, dend date, iddatapropval bigint, parentdatapropval bigint, numberval double precision, 
                strval character varying(1000), multistrval text, datetimeval timestamp without time zone,
                propval bigint, obj bigint, relobj bigint, name character varying(4000), fileval bigint, 
                "timestamp" timestamp without time zone)
        """)
    }
    //
    private Store loadSqlMeta(String sql, String domain) {
        return apiMeta().get(ApiMeta).loadSql(sql, domain)
    }

    private Store loadSqlMetaWithParams(String sql, String domain, Map<String, Object> params) {
        return apiMeta().get(ApiMeta).loadSqlWithParams(sql, domain, params)
    }

    private void saveCubeInfo(Map<String, Object> params) {
        apiMetaCube().get(ApiMetaCube).saveCubeInfo(params)
    }

}
