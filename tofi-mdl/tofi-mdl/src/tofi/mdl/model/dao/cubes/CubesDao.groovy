package tofi.mdl.model.dao.cubes

import jandcode.commons.UtCnv
import jandcode.commons.error.XError
import jandcode.core.dao.DaoMethod
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreField
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import org.codehaus.groovy.util.ListHashMap
import tofi.mdl.consts.FD_CubeSDimType_consts
import tofi.mdl.consts.FD_DimObjItemType_consts
import tofi.mdl.consts.FD_DimPropType_consts
import tofi.mdl.model.utils.EntityMdbUtils

class CubesDao extends BaseMdbUtils {

    @DaoMethod
    Store loadCubes(long cubesGr, long cubes) throws Exception {
        Store st = mdb.createStore("CubeS")
        String whe = "cubesGr=${cubesGr}"
        if (cubes > 0)
            whe = "id=${cubes}"
        return mdb.loadQuery(st, """
            select *
            from cubes
            where ${whe}
            order by ord
        """)
    }

    @DaoMethod
    Store insertCube(Map<String, Object> params) throws Exception {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "CubeS")
        long id = eu.insertEntity(params)
        return loadCubes(0, id)
    }

    @DaoMethod
    Store updateCube(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"))
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "CubeS")
        eu.updateEntity(params)
        return loadCubes(0, id)
    }

    @DaoMethod
    void deleteCube(Map<String, Object> params) {
        EntityMdbUtils eu = new EntityMdbUtils(mdb, "CubeS")
        eu.deleteEntity(params)
    }

    @DaoMethod
    Store loadRec(long cubes) {
        Store st = mdb.createStore("CubeS")
        return mdb.loadQuery(st, """
            select *
            from cubes
            where id=:id
        """, [id: cubes])
    }

    @DaoMethod
    Store loadDims(long cubes, long id) {
        Store st = mdb.createStore("CubeSDim.full")
        String whe = "c.cubes=${cubes}"
        if (id > 0)
            whe = "c.id=${id}"

        return mdb.loadQuery(st, """
            select c.*,
            case 
                when c.dimperiod is not null then dpe.name
                when c.dimprop is not null then dpr.name
                when c.dimobj is not null then dob.name
            end as name, dpr.dimPropType, dpt.text as dimPropTypeName
            from cubesdim c
                left join DimPeriod dpe on c.dimperiod=dpe.id
                left join DimProp dpr on c.dimprop=dpr.id
                left join DimObj dob on c.dimobj=dob.id
                left join FD_DimPropType dpt on dpt.id=dpr.dimproptype
            where ${whe}
            order by c.cubesDimType
        """)
    }

    @DaoMethod
    Store saveCubeDim(Map<String, Object> params) {
        String mode = UtCnv.toString(params.get("mode"))
        if (mode == "ins") {
            if (UtCnv.toLong(params.get("cubeSDimType") == FD_CubeSDimType_consts.period)) {
                Store st = mdb.loadQuery("""
                select id from CubeSDim
                where cubeSDimType=:cdt and cubeS=:c
            """, [cdt: FD_CubeSDimType_consts.period, c: UtCnv.toLong(params.get("cubeS"))])
                if (st.size() > 0)
                    throw new XError("DimPeriodExists")
            }

            if (UtCnv.toLong(params.get("dimPropType") == FD_DimPropType_consts.factor)) {
                Store st = mdb.loadQuery("""
                select id from CubeSDim
                where cubeSDimType=:cdt and cubeS=:c
            """, [cdt: FD_CubeSDimType_consts.prop, c: UtCnv.toLong(params.get("cubeS"))])
                if (st.size() == 0)
                    throw new XError("DimPropMissing")
            }
        }

/*        if (params.get("mode")=="ins" && UtCnv.toLong(params.get("cubeSDimType"))==FD_CubeSDimType_consts.obj) {
            Store stTmp = mdb.loadQuery("""
                select * from CubeSFace where cubeS=:c
            """, [c: UtCnv.toLong(params.get("cubeS"))])
            if (stTmp.size() > 0)
                throw new XError("CubeSFaceExists")
        }*/

        params.remove("name")
        params.remove("dimPropType")
        params.remove("dimPropTypeName")
        params.remove("mode")
        long id
        if (mode == "ins") {
            id = mdb.insertRec("CubeSDim", params, true)
        } else {
            id = UtCnv.toLong(params.get("id"))
            mdb.updateRec("CubeSDim", params)
        }
        return loadDims(0, id)
    }

    @DaoMethod
    Store loadDimPeriod() {
        return mdb.loadQuery("""
            select id, name
            from DimPeriod
            where 0=0
        """)
    }

    @DaoMethod
    Store loadDimProp(long cubeS) {
        return mdb.loadQuery("""
            select d.id, d.name, d.dimPropType  
            from cubesdim c
                left join dimprop d on c.dimprop=d.id
            where c.cubes=${cubeS} and c.dimprop is not null
            order by d.id
        """)
    }


    @DaoMethod
    Store loadDimPropForSelect(long cubeS, long dimPropType, String mode) {
        Store st = mdb.createStore("Dims.select")
        String whe = ""
        if (dimPropType == FD_DimPropType_consts.prop && mode == "ins") {
            whe = """
                and id not in (
                    select dimprop 
                    from cubesdim
                    where cubes=${cubeS} and dimprop is not null            
                )
            """
        }
        return mdb.loadQuery(st, """
            select -id as id, parent, name, 0 as dimProp
            from DimPropGr
            union all
            select id, -dimPropgr as parent, name, id as dimProp
            from DimProp
            where dimPropType=${dimPropType} ${whe} 
        """)
    }

    @DaoMethod
    Store loadDimPropItem(long dimProp) {
        return mdb.loadQuery("""
            select dpi.id, dpi.parent, dpi.name, dp.dimproptype,
            case 
                when dp.dimproptype=1 then dpi.prop
                when dp.dimproptype=3 then dpi.multiprop
                when dp.dimproptype=4 then dpi.dimMultiPropItem
            end as val
            from DimPropItem dpi
                left join DimProp dp on dpi.dimProp=dp.id
            where dpi.dimprop=${dimProp}
            order by dpi.ord
        """)
    }

    @DaoMethod
    Store loadDimPropItemFV(long dimProp) {
        return mdb.loadQuery("""
            select dpi.id, dpi.parent, dpi.name, null as dimPropItemFV, null as factorVal 
            from DimPropItem dpi
                left join DimProp dp on dpi.dimProp=dp.id
            where dpi.dimprop=${dimProp}
            order by dpi.ord
        """)

        /*
select dimpropitem,
	STRING_AGG (cast(id as varchar(1000)), ',' order by id) as dpifv,
	STRING_AGG (cast(factorval as varchar(1000)), ',' order by id) as fv
from DimPropItemFV
where dimpropitem in (1010,1011,1012,1014)
group by dimpropitem
         */
    }

    //--------------------------------

    @DaoMethod
    Store loadDimObj() {
        Store st = mdb.createStore("Dims.select")
        return mdb.loadQuery(st, """
            select -id as id, parent, name, 0 as dimObj
            from DimObjGr
            union all
            select id, -dimObjgr as parent, name, id as dimObj
            from DimObj
            where 0=0
        """)
    }

    @DaoMethod
    Map<String, Object> loadDimObjForSelect(long cube) {
        long idCubeFace
        Store stCubeSDim = mdb.loadQuery("""
            select d.id, d.name
            from cubesdim cd
            left join DimObj d on d.id=cd.dimobj
            where cd.cubes=${cube} and cd.dimobj is not null
            order by cd.id
        """)
        Store stCubeSFace = mdb.loadQuery("""
            select *
            from CubesFace
            where cubes=${cube}
        """)
        if (stCubeSFace.size()==0) {
            StoreRecord rec = mdb.createStoreRecord("CubeSFace")
            rec.set("cubeS", cube)
            rec.set("countDim", stCubeSDim.size())
            idCubeFace = mdb.insertRec("CubeSFace", rec, true)
            //
            for (StoreRecord r in stCubeSDim) {
                rec = mdb.createStoreRecord("CubeSFaceDim")
                rec.set("cubeSFace", idCubeFace)
                rec.set("dimObj", r.getLong("id"))
                mdb.insertRec("CubeSFaceDim", rec, true)
            }
        } else {
            idCubeFace = stCubeSFace.get(0).getLong("id")
            if (stCubeSFace.get(0).getInt("countDim") != stCubeSDim.size()) {
                mdb.execQuery("""
                    update CubeSFace set countDim=:cd where id=:id
                """, [cd: stCubeSDim.size(), id: idCubeFace])
                //
                Store stCubeSFaceDim = mdb.loadQuery("""
                    select * from CubeSFaceDim where cubeSFace=:cf
                """, [cf: stCubeSFace.get(0).getLong("id")])
                Set<Object> ids = stCubeSFaceDim.getUniqueValues("dimObj")
                for (StoreRecord r in stCubeSDim) {
                    if (ids.contains(r.getLong("dimObj")))
                        continue
                    StoreRecord rec = mdb.createStoreRecord("CubeSFaceDim")
                    rec.set("cubeSFace", stCubeSFace.get(0).getLong("id"))
                    rec.set("dimObj", r.getLong("id"))
                    mdb.insertRec("CubeSFaceDim", rec, true)
                }
            }
        }
        return [cubeSFace: idCubeFace, store: stCubeSDim]
    }

    @DaoMethod
    Map<String, Store> loadAllDimObjForFixed(Map<String, Object> params) {
        Map<String, Store> res = new HashMap<>()
        long cube = UtCnv.toLong(params.get("cubeS"))
        long do3 = UtCnv.toLong(params.get("dimObj3"))
        long do4 = 0
        if ("dimObj4" in params) {
            do4 = UtCnv.toLong(params.get("dimObj4"))
        }
        Store st = loadDimObjForFixed(do3)

        //mdb.outTable(st)

        res.put("doi3", st)
        if (do4 > 0) {
            st = loadDimObjForFixed(do4)
            res.put("doi4", st)
        }
        return res
    }


    private void fillStCls(long doi, long typ, Store stCls) {
        Set<Object> ids = stCls.getUniqueValues("id")
        mdb.loadQuery(stCls, """
            select c.id, v.name, ${doi} as doi from Cls c, ClsVer v 
            where c.id=v.ownerVer and v.lastVer=1 and c.typ=${typ} and c.id not in (0${ids.join(",")})
        """)
    }


    private Store loadDimObjForFixed(long dimObj) {
        //todo dimObjItemType: 1,2,3,4,5,...
        Store stDOI = mdb.loadQuery("""
            select * from DimObjItem where dimobj=${dimObj}
        """)
        Store stCls = mdb.createStore("IdName")

        //dimObjItemType:   typ,cls,reltyp,relcls,utyp,ucls,ureltyp,urelcls
        //                  1   2   3       4       5   6       7       8


        for (StoreRecord rec in stDOI) {
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.typ) {
                fillStCls(rec.getLong("id"), rec.getLong("typ"), stCls)
            }
            //
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.cls) {
                throw new XError("dimObjItemType: Cls")
            }
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.reltyp) {
                throw new XError("dimObjItemType: RelTyp")
            }
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.relcls) {
                throw new XError("dimObjItemType: RelCls")
            }
            //
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.utyp) {
                Store stTmp = mdb.loadQuery("""
                    select * from reltypmember where id=${rec.getLong("relTypMember")}
                """)
                fillStCls(rec.getLong("id"), stTmp.get(0).getLong("typ"), stCls)
            }
            //
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.ucls) {
                throw new XError("dimObjItemType: UCls")
            }
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.ureltyp) {
                throw new XError("dimObjItemType: URelTyp")
            }
            if (rec.getLong("dimObjItemType") == FD_DimObjItemType_consts.urelcls) {
                throw new XError("dimObjItemType: URelCls")
            }
            //
        }

        //mdb.outTable(stCls)

        return stCls
    }

    @DaoMethod
    void checkDimProp(long cube) {
        Store st = mdb.loadQuery("""
            select id from cubesdim where cubes=:c and cubesdimtype=:cdt
        """, [c: cube, cdt: FD_CubeSDimType_consts.prop])
        if (st.size() == 0)
            throw new XError("DimPropNotExists")
    }

    @DaoMethod
    void checkDimObj(long cube) {
        Store st = mdb.loadQuery("""
            select id from cubesdim where cubes=:c and cubesdimtype=:cdt
        """, [c: cube, cdt: FD_CubeSDimType_consts.obj])
        if (st.size() == 0)
            throw new XError("DimObjNotExists")

        if (st.size() == 1)
            throw new XError("DimObjFaceNotExists")
    }

    @DaoMethod
    void deleteCubeDim(long id) {
        Store stTmp = mdb.loadQuery("""
            select cubeS from CubeSDim where id=:id and cubeSDimType=:cdt
        """, [id: id, cdt: FD_CubeSDimType_consts.obj])
        if (stTmp.size() > 0) {
            stTmp = mdb.loadQuery("""
                select * from CubeSFace where cubeS=:c 
            """, [c: stTmp.get(0).getLong("cubeS")])

            if (stTmp.size() > 0)
                throw new XError("needDeleteProcubeObj")
        }


        mdb.deleteRec("CubeSDim", id)
    }

    private static Map<String, Object> initMapDims(Map<Long, Long> params) {
        long dimAll = 0
        Map<Long, Long> mapDimsOther = new ListHashMap<>()

        for (Map.Entry map in params.entrySet()) {
            if (UtCnv.toLong(map.value) == 1)
                dimAll = UtCnv.toLong(map.key)
            if (UtCnv.toLong(map.value) > 1) {
                mapDimsOther.put(UtCnv.toLong(map.key), UtCnv.toLong(map.value))
            }
        }
        return [dimAll: dimAll, dimsOther: mapDimsOther]
    }

    @DaoMethod
    Map<String, Object> getGrid(long cubeS, Map<Long, Long> params) {
        def cols = []

        Map<String, Object> mapDims = initMapDims(params)
        long dimAll = mapDims.get("dimAll") as long
        Map<Long, Long> mapDimsOther = mapDims.get("dimsOther") as Map<Long, Long>

        Store st0 = mdb.loadQuery("select id, dimproptype, name from DimProp where id=${dimAll}")
        String propField = "prop"
        if (st0.get(0).getLong('dimPropType') == FD_DimPropType_consts.multiList ||
                st0.get(0).getLong('dimPropType') == FD_DimPropType_consts.dimMultiProp) {
            propField = "multiProp"
        }
        if (st0.get(0).getLong('dimPropType') == FD_DimPropType_consts.factor) {
            Store st_ = mdb.loadQuery("""
                select id, dimproptype, name
                from DimProp where dimPropType <> ${FD_DimPropType_consts.factor} 
                    and id in (0${mapDimsOther.keySet().join(",")})
            """)
            //st_.size > 0 !
            if (st_.size() == 0)
                throw new XError("DimPropMissing")
            if (st_.get(0).getLong('dimPropType') == FD_DimPropType_consts.prop)
                propField = "prop"
            else
                propField = "multiProp"
        }
        //
        cols.add(Map.of("name", "name", "label", st0.get(0).getString('name'), "field", "name",
                "align", "left", "headerStyle", "font-size: 1.2em; width: 60%",
                "headerClass", "text-bold text-white bg-blue-grey-13"))
        cols.add(Map.of("name", "val", "label", "Свойство", "field", "val",
                "align", "left", "headerStyle", "font-size: 1.2em; width: 40%",
                "headerClass", "text-bold text-white bg-blue-grey-13"))
        //
        Store stRes = mdb.createStore("PropFace.prop")
        String sql = """
            select dpi.id, dpi.parent, dpi.name, null as val
            from DimPropItem dpi
                left join DimProp dp on dp.id=dpi.dimprop
                        where dpi.dimprop=${dimAll}
        """

        boolean showAction = true
        boolean showSelect = true
        if (st0.get(0).getLong('dimPropType') != FD_DimPropType_consts.factor) {

/*            if (mapDimsOther.size() == 0) {
                showAction = false
                showSelect = false
                sql = """
                    select dpi.id, dpi.parent, dpi.name, ${propField} as val
                    from DimPropItem dpi
                        left join DimProp dp on dp.id=dpi.dimprop
                                where dpi.dimprop=${dimAll}
                """
            } else {
                Store st_ = mdb.loadQuery("""
                    select id, dimproptype, name
                    from DimProp where dimPropType <> ${FD_DimPropType_consts.factor} 
                        and id in (0${mapDimsOther.keySet().join(",")})
                """)
                if (st_.size() > 0) {
                    //showAction = false
                    //showSelect = true
                    sql = """
                        select dpi.id, dpi.parent, dpi.name, multiProp as val
                        from DimPropItem dpi
                            left join DimProp dp on dp.id=dpi.dimprop
                                    where dpi.dimprop=${dimAll}
                    """
                }
            }*/

            Map<String, Object> map = loadCubeSPropCell(cubeS, params, propField)
            stRes = map["store"] as Store

            return Map.of("cols", cols, "rows", stRes, "propField", propField,
                    "showAction", showAction, "showSelect", showSelect)
        } else {
            mdb.loadQuery(stRes, sql)
            // find val!


            return Map.of("cols", cols, "rows", stRes, "propField", propField,
                    "showAction", showAction, "showSelect", showSelect)
        }
    }

    @DaoMethod
    Map<String, Object> binding(long cubeS, String propField, Map<Long, Long> params) {

        Map<String, Object> mapDims = initMapDims(params)
        long dimAll = mapDims.get("dimAll") as long
        Map<Long, Long> mapDimsOther = mapDims.get("dimsOther") as Map<Long, Long>

        def lstDPI = []
        for (Map.Entry map in mapDimsOther.entrySet()) {
            lstDPI.add(UtCnv.toLong(map.value))
        }

        if (lstDPI.size()>0) {
            Store stFV = mdb.loadQuery("""
                select fv.factorval as fv
                from DimPropItem dpi
                    left join DimPropItemFV fv on dpi.id=fv.dimpropitem
                    left join factor f on f.id=fv.factorval
                where dpi.id in (0${lstDPI.join(",")})
                order by f.ord
            """)

            Set<Object> lstFV = stFV.getUniqueValues("fv")

            Store stMeter = mdb.loadQuery("""
                select dpi.id as dpi, p.meter as meter, p.id as prop1
                from DimPropItem dpi
                    left join DimProp dp on dp.id=dpi.dimprop
                    left join Prop p on p.id=dpi.prop
                where dpi.dimprop=${dimAll}
            """)

            Store stProp = mdb.createStore("PropFace.bind")
            for (StoreRecord r in stMeter) {
                long meter = r.getLong("meter")
                long prop1 = r.getLong("prop1")

                Store st = mdb.loadQuery("""
                    with mr as (
                        select mr.id as mr, ${prop1} as prop1 
                        from meterrate mr
                        where mr.meter=${meter}
                    ),
                    fv as (
                        select 
                            meterrate,  mr.prop1,
                            string_agg (cast(mrfv.factorval as varchar(4000)), ',' order by fv.ord) as fvlist
                        from meterratefv mrfv
                            left join factor as fv on mrfv.factorval=fv.id
                            inner join mr on mr.mr=mrfv.meterrate
                        where 0=0
                        group by mrfv.meterrate, mr.prop1 
                    )
                    select fv.*, p.id as prop 
                    from fv left join prop p on fv.meterrate=p.meterrate
                    where fvlist='${lstFV.join(",")}'
                """)
                stProp.add(st)
            }
            //mdb.outTable(stProp)
            StoreIndex indStProp = stProp.getIndex("prop1")

            for (StoreRecord r in stMeter) {
                StoreRecord rec = indStProp.get(r.getLong("prop1"))
                if (rec != null) {
                    StoreRecord rCPC = mdb.createStoreRecord("CubeSPropCell")
                    rCPC.set("cubeS", cubeS)
                    rCPC.set(propField, rec.getLong("prop"))
                    long idCPC = mdb.insertRec("CubeSPropCell", rCPC, true)
                    //
                    StoreRecord rCPCI = mdb.createStoreRecord("CubeSPropCellItem")
                    rCPCI.set("cubeSPropCell", idCPC)
                    rCPCI.set("dimPropItem", r.getLong("dpi"))
                    mdb.insertRec("CubeSPropCellItem", rCPCI, true)

                    for (long cpci in lstDPI) {
                        rCPCI = mdb.createStoreRecord("CubeSPropCellItem")
                        rCPCI.set("cubeSPropCell", idCPC)
                        rCPCI.set("dimPropItem", cpci)
                        mdb.insertRec("CubeSPropCellItem", rCPCI, true)
                    }
                }
            }
        } else {
            Store stProp = mdb.loadQuery("""
                select id, prop, multiprop from dimpropitem
                where dimprop=${dimAll}
                order by ord
            """)

            for (StoreRecord r in stProp) {
                StoreRecord rCPC = mdb.createStoreRecord("CubeSPropCell")
                rCPC.set("cubeS", cubeS)
                rCPC.set(propField, r.getLong("prop"))
                long idCPC = mdb.insertRec("CubeSPropCell", rCPC, true)
                //
                StoreRecord rCPCI = mdb.createStoreRecord("CubeSPropCellItem")
                rCPCI.set("cubeSPropCell", idCPC)
                rCPCI.set("dimPropItem", r.getLong("id"))
                mdb.insertRec("CubeSPropCellItem", rCPCI, true)
            }
        }

        return loadCubeSPropCell(cubeS, params, propField)
    }

    @DaoMethod
    Map<String, Object> loadCubeSPropCell(long cubeS, Map<Long, Long> dims, String propField) {

        Map<String, Object> mapDims = initMapDims(dims)
        long dimAll = mapDims.get("dimAll") as long
        Map<Long, Long> mapDimsOther = mapDims.get("dimsOther") as Map<Long, Long>

        Store stPropOfDims = mdb.loadQuery("""
            select c.${propField},
                string_agg (cast(ci.dimpropitem as varchar(1000)), '_' order by dpi.id) as dims
            from CubeSPropCell c
                left join CubeSPropCellItem ci on ci.cubespropcell=c.id
                left join DimPropItem dpi on ci.dimpropitem=dpi.id
            where c.cubes=${cubeS}
            group by c.${propField}
            having count(*)=${mapDimsOther.size() + 1}
        """)

        StoreIndex indPropOfDims = stPropOfDims.getIndex("dims")

        Store stRes = mdb.loadQuery("""
            select dpi.id, dpi.parent, dpi.name, null as val
            from DimPropItem dpi
                left join DimProp dp on dp.id=dpi.dimprop
            where dpi.dimprop=${dimAll}
        """)

        for (StoreRecord rec in stRes) {
            List<Long> lstDim = new ArrayList<>()
            for (def map in mapDimsOther.entrySet()) {
                lstDim.add(UtCnv.toLong(map.value))
            }
            lstDim.add(rec.getLong("id"))

            lstDim.sort()
            String dim = lstDim.join("_")
            StoreRecord rr = indPropOfDims.get(dim)
            if (rr != null) {
                if (rr.getLong(propField) > 0)
                    rec.set("val", rr.getLong(propField))
            }
        }
        //mdb.outTable(stRes)

        return [store: stRes]
    }

    @DaoMethod
    void unbinding(long cubeS, Map<Long, Long> mapping) {
        Map<String, Object> mapDims = initMapDims(mapping)
        long dimAll = mapDims.get("dimAll") as long
        Map<Long, Long> mapDimsOther = mapDims.get("dimsOther") as Map<Long, Long>

        Store stDPI0 = mdb.loadQuery("""
            select id from DimPropItem where dimProp=${dimAll} order by id
        """)
        Set<Object> setDPI0 = stDPI0.getUniqueValues("id")
        for (Object e in setDPI0) {
            def lstDPI = []
            for (Map.Entry map in mapDimsOther.entrySet()) {
                lstDPI.add(UtCnv.toLong(map.value))
            }
            lstDPI.add(UtCnv.toLong(e))
            lstDPI.sort()

            Store st = mdb.loadQuery("""
                with cpci as (
                    select i.cubespropcell, 
                    STRING_AGG (cast(i.dimpropitem as varchar(4000)), '_' order by i.dimpropitem) as dpi,
                    STRING_AGG (cast(i.id as varchar(4000)), ',') as ids
                    from CubeSPropCell c
                    left join CubeSPropCellItem i on c.id=i.cubespropcell 
                where c.cubes=${cubeS}
                group by i.cubespropcell
                )
                select * from cpci
                where dpi='${lstDPI.join("_")}'
            """)
            if (st.size() > 0) {
                mdb.execQuery("""
                    delete from CubeSPropCellItem
                    where id in (${st.get(0).getString("ids")});
                """)
            }

        }
        mdb.execQuery("""
            delete from CubesPropCell
            where id in (
                select id from CubesPropCell
                except
                select cubesPropCell as id from CubesPropCellItem
            )
        """)
    }

    @DaoMethod
    void selectValue(long cubeS, long dimi0, long val, Map<String, Object> mapping, String propField) {
        if (val <= 0 || dimi0 == 0) return
        List<Long> dimi_ = new ArrayList<>()

        for (Map.Entry map in mapping.entrySet()) {
            if (UtCnv.toLong(map.value) > 1) {
                dimi_.add(UtCnv.toLong(map.value))
            }
        }
        dimi_.add(dimi0)
        dimi_.sort()

        Store st = mdb.loadQuery("""
            with di as (
            select c.id, c.prop,
                string_agg (cast(ci.dimpropitem as varchar(1000)), '_' order by dpi.id) as dimi_
            from CubeSPropCell c
                left join CubeSPropCellItem ci on ci.cubespropcell=c.id
                left join DimPropItem dpi on ci.dimpropitem=dpi.id
            where c.cubes=${cubeS}
            group by c.id, c.prop
            having count(*)=${dimi_.size()}
            )
            select * from di where dimi_='${dimi_.join("_")}'
        """)
        if (st.size() > 0) { //upd
            mdb.execQuery("""
                update CubeSPropCell set ${propField}=${val}
                where id=${st.get(0).getLong("id")}
            """)
        } else { //ins
            StoreRecord rCPC = mdb.createStoreRecord("CubeSPropCell")
            rCPC.set("cubeS", cubeS)
            rCPC.set(propField, val)
            long idCPC = mdb.insertRec("CubeSPropCell", rCPC, true)
            //
            dimi_.forEach { Long it ->
                StoreRecord rCPCI = mdb.createStoreRecord("CubeSPropCellItem")
                rCPCI.set("cubeSPropCell", idCPC)
                rCPCI.set("dimPropItem", it)
                mdb.insertRec("CubeSPropCellItem", rCPCI, true)
            }

        }
    }

    //------------------
    @DaoMethod
    Map<String, Object> loadGridDO(long cubeS, int countDimObj, long do1, long do2, long cl3, long cl4) {
        Map<String, Object> res = new HashMap<>()
        Store st1 = loadDimObjForFixed(do1)
        Store st2 = loadDimObjForFixed(do2)
        def cols = []
        int cnt = UtCnv.toInt(100 / (st2.size() + 1))
        cols.add(Map.of("name", "name", "label", "", "field", "name",
                "align", "left", "headerStyle", "font-size: 1.2em; width: " + cnt + "%",
                "headerClass", "text-bold text-white bg-blue-grey-13"))


        for (StoreRecord r in st2) {
            cols.add(Map.of("name", r.getString("id") + "_" + r.getString("doi"), "label", r.getString('name'),
                    "field", r.getString("id") + "_" + r.getString("doi"),
                    "align", "left", "headerStyle", "font-size: 1.2em; width: " + cnt + "%",
                    "headerClass", "text-bold text-white bg-blue-grey-13"))

        }
        res.put("cols", cols)
        //
        //System.out.println(cols)

        for (Object f in cols) {
            if (f["label"] != "") {
                st1.addField(f["name"].toString(), long)
            }
        }

        Set<Object> ids1 = st1.getUniqueValues("id")
        Set<Object> ids2 = st2.getUniqueValues("id")
        Map<String, Store> mapSel = new HashMap<>()

        for (Object c1 in ids1) {
            for (Object c2 in ids2) {
                long c1i = UtCnv.toLong(c1)
                long c2i = UtCnv.toLong(c2)
                int count = 0
                String whe = ""
                if (cl3 == 0 && cl4 == 0) {
                    count = 2
                    whe = "array[res.clist] @> array [${c1i}]::text[] and array[res.clist] @> array [${c2i}]::text[]"
                } else if (cl3 > 0 && cl4 == 0) {
                    count = 3
                    whe = "array[res.clist] @> array [${c1i}]::text[] and array[res.clist] @> array [${c2i}]::text[] and array[res.clist] @> array [${cl3}]::text[]"
                } else if (cl3 == 0 && cl4 > 0) {
                    count = 3
                    whe = "array[res.clist] @> array [${c1i}]::text[] and array[res.clist] @> array [${c2i}]::text[] and array[res.clist] @> array [${cl4}]::text[]"
                } else if (cl3 > 0 && cl4 > 0) {
                    count = 4
                    whe = "array[res.clist] @> array [${c1i}]::text[] and array[res.clist] @> array [${c2i}]::text[] and array[res.clist] @> array [${cl3}]::text[] and array[res.clist] @> array [${cl4}]::text[]"
                }

                Store stSel = mdb.loadQuery("""
                    with res as (
                        select relcls, count(*) as cnt,
                            string_to_array(string_agg (cast(cls as varchar(1000)), ','), ',') as clist
                        from relclsmember
                        group by relcls 
                        having count(*)=${count}
                    )
                    select res.relcls as id, rv.name, rc.reltyp
                    from res, relcls rc, relclsver rv
                    where res.relcls=rc.id and rc.id=rv.ownerver and rv.lastver=1
                        and ${whe}
                """)
                if (cl3 == 0 && cl4 == 0)
                    mapSel.put("" + c1i + "_" + c2i, stSel)
                else if (cl3 > 0 && cl4 == 0)
                    mapSel.put("" + c1i + "_" + c2i + "_" + cl3, stSel)
                else if (cl3 > 0 && cl4 > 0)
                    mapSel.put("" + c1i + "_" + c2i + "_" + cl3 + "_" + cl4, stSel)
            }
        }

        //mdb.outTable(st1)
        //restore RelCls
        Store stVal = mdb.loadQuery("""
            select v.relcls, v.dimobjitemcomb as comb 
            from CubeSFace f
                left join CubeSFaceValue v on f.id=v.cubesface
            where f.cubeS=${cubeS} and f.countdim=${countDimObj}
        """)
        StoreIndex indVal = stVal.getIndex("comb")


        for (StoreRecord r : st1) {
            for (StoreField f : r.getFields()) {
                if (f.getName().contains("_")) {
                    String cl2 = f.getName().split("_")[0]
                    StoreRecord rec = indVal.get(r.getString("id") + "_" + cl2)
                    if (cl3 > 0 && cl4 == 0)
                        rec = indVal.get(r.getString("id") + "_" + cl2 + "_" + cl3)
                    if (cl3 > 0 && cl4 > 0)
                        rec = indVal.get(r.getString("id") + "_" + cl2 + "_" + cl3 + "_" + cl4)
                    if (rec != null) {
                        r.set(f.getName().toString(), rec.getLong("relcls"))
                    }
                }
            }
        }

        //mdb.outTable(stVal)
        //mdb.outTable(st1)
        //
        res.put("rows", st1)
        res.put("sel", mapSel)
        return res
    }

    private void insCubeSFaceValue(long cubeSFace, long rc, long rt,
                              long doi1, long doi2, long doi3, long doi4, String doic) {

        StoreRecord rec = mdb.createStoreRecord("CubeSFaceValue")
        rec.set("cubeSFace", cubeSFace)
        rec.set("relTyp", rt)
        rec.set("relCls", rc)
        rec.set("dimObjItemComb", "${doic}")
        long idCubeFaceValue = mdb.insertRec("CubeSFaceValue", rec, true)
        //
        for (long doi in [doi1, doi2, doi3, doi4]) {
            if (doi == 0) continue
            rec = mdb.createStoreRecord("CubeSFaceValueAsgn")
            Store stDOI = mdb.loadQuery("""
                select relTypMember, relClsMember from DimObjItem where id=${doi}
            """)
            long relTypMember = stDOI.get(0).getLong("relTypMember")
            long relClsMember = stDOI.get(0).getLong("relClsMember")
            rec.set("cubeSFaceValue", idCubeFaceValue)
            if (relTypMember > 0)
                rec.set("relTypMember", relTypMember)
            if (relClsMember > 0)
                rec.set("relClsMember", relClsMember)
            rec.set("dimObjItem", doi)
            mdb.insertRec("CubeSFaceValueAsgn", rec, true)
        }

    }

    @DaoMethod
    void saveCubesFace(long cubeSFace, long rc, long rt, long doi1, long doi2, long cl1, long cl2,
                       long doi3, long cl3, long doi4, long cl4) {

        String doic = "${cl1}_${cl2}"
        if (doi3 != 0 && doi4 == 0)
            doic = "${cl1}_${cl2}_${cl3}"
        if (doi3 != 0 && doi4 != 0)
            doic = "${cl1}_${cl2}_${cl3}_${cl4}"

        Store stVal = mdb.loadQuery("""
            select id 
            from CubeSFaceValue
            where cubesface=${cubeSFace} and dimobjitemcomb='${doic}'
        """)

        if (stVal.size() > 0) {
            long idCubeSFaceValue = stVal.get(0).getLong("id")
            mdb.execQuery("""
                update CubeSFaceValue set relTyp=${rt}, relCls=${rc}
                where id=${idCubeSFaceValue}
            """)
        } else {
            insCubeSFaceValue(cubeSFace, rc, rt, doi1, doi2, doi3, doi4, doic)
        }
    }


    @DaoMethod
    void unbindingDO(long cubeSFace, long do1, long do2, long cl3, long cl4) {
        Store st1 = loadDimObjForFixed(do1)
        Store st2 = loadDimObjForFixed(do2)
        Set<Object> ids1 = st1.getUniqueValues("id")
        Set<Object> ids2 = st2.getUniqueValues("id")

        for (Object c1 in ids1) {
            for (Object c2 in ids2) {
                long c1i = UtCnv.toLong(c1)
                long c2i = UtCnv.toLong(c2)
                String doic = ""
                if (cl3 == 0 && cl4 == 0) {
                    doic = "${c1i}_${c2i}"
                } else if (cl3 > 0 && cl4 == 0) {
                    doic = "${c1i}_${c2i}_${cl3}"
                } else if (cl3 > 0 && cl4 > 0) {
                    doic = "${c1i}_${c2i}_${cl3}_${cl4}"
                }
                mdb.execQuery("""
                    with cfv as (
                        select id from CubeSFaceValue
                        where cubeSFace=:cf and dimObjItemComb=:doic
                    )
                    delete from CubeSFaceValueAsgn
                    where cubeSFaceValue in (select id from cfv);
                    delete from CubeSFaceValue
                    where cubeSFace=:cf and dimObjItemComb=:doic;
                """, [cf: cubeSFace, doic: doic])
            }
        }
    }

    @DaoMethod
    void bindingDO(long cubeSFace, int dimCount, long do1, long do2, long doi3, long doi4, long cl3, long cl4) {
        Store st1 = loadDimObjForFixed(do1)
        Store st2 = loadDimObjForFixed(do2)
        def cols = []
        for (StoreRecord r in st2) {
            cols.add(Map.of("name", r.getString("id") + "_" + r.getString("doi"), "label", r.getString('name')))
        }
        for (Object f in cols) {
            st1.addField(f["name"].toString(), long)
        }
        Set<Object> ids1 = st1.getUniqueValues("id")
        Set<Object> ids2 = st2.getUniqueValues("id")
        Map<String, Store> mapSel = new HashMap<>()

        for (Object c1 in ids1) {
            for (Object c2 in ids2) {
                long c1i = UtCnv.toLong(c1)
                long c2i = UtCnv.toLong(c2)
                int count = 0
                String whe = ""
                if (cl3 == 0 && cl4 == 0) {
                    count = 2
                    whe = "array[res.clist] @> array [${c1i}]::text[] and array[res.clist] @> array [${c2i}]::text[]"
                } else if (cl3 > 0 && cl4 == 0) {
                    count = 3
                    whe = "array[res.clist] @> array [${c1i}]::text[] and array[res.clist] @> array [${c2i}]::text[] and array[res.clist] @> array [${cl3}]::text[]"
                } else if (cl3 > 0 && cl4 > 0) {
                    count = 4
                    whe = "array[res.clist] @> array [${c1i}]::text[] and array[res.clist] @> array [${c2i}]::text[] and array[res.clist] @> array [${cl3}]::text[] and array[res.clist] @> array [${cl4}]::text[]"
                }

                Store stSel = mdb.loadQuery ("""
                    with res as (
                        select relcls, count(*) as cnt,
                            string_to_array(string_agg (cast(cls as varchar(1000)), ','), ',') as clist
                        from relclsmember
                        group by relcls 
                        having count(*)=${count}
                    )
                    select res.relcls as id, rv.name, rc.reltyp
                    from res, relcls rc, relclsver rv
                    where res.relcls=rc.id and rc.id=rv.ownerver and rv.lastver=1
                        and ${whe}
                """)
                if (cl3 == 0 && cl4 == 0)
                    mapSel.put("" + c1i + "_" + c2i, stSel)
                else if (cl3 > 0 && cl4 == 0)
                    mapSel.put("" + c1i + "_" + c2i + "_" + cl3, stSel)
                else if (cl3 > 0 && cl4 > 0)
                    mapSel.put("" + c1i + "_" + c2i + "_" + cl3 + "_" + cl4, stSel)
            }
        }

        //mdb.outTable(st1)
        Store stSel
        for (StoreRecord r : st1) {
            for (StoreField f : r.getFields()) {
                if (f.getName().contains("_")) {
                    String cl2 = f.getName().split("_")[0]
                    long doi_2 = UtCnv.toLong(f.getName().split("_")[1])
                    if (dimCount == 2 || (dimCount == 3 && doi3 == 0)) {
                        stSel = mapSel.get(r.getString("id") + "_" + cl2)
                        if (stSel.size() == 1) {
                            saveCubesFace(cubeSFace,
                                    stSel.get(0).getLong("id"),
                                    stSel.get(0).getLong("reltyp"),
                                    r.getLong("doi"), doi_2, r.getLong("id"), UtCnv.toLong(cl2),
                                    0L, 0L, 0L, 0L)
                        }
                    }
                    if (dimCount == 3 && doi3 > 0) {
                        stSel = mapSel.get(r.getString("id") + "_" + cl2 + "_" + cl3)
                        if (stSel.size() == 1) {
                            saveCubesFace(cubeSFace,
                                    stSel.get(0).getLong("id"),
                                    stSel.get(0).getLong("reltyp"),
                                    r.getLong("doi"), doi_2, r.getLong("id"), UtCnv.toLong(cl2),
                                    doi3, cl3, 0L, 0L)
                        }
                    }
                    if (dimCount == 4 && doi3 == 0) {
                        stSel = mapSel.get(r.getString("id") + "_" + cl2)
                        if (stSel.size() == 1) {
                            saveCubesFace(cubeSFace,
                                    stSel.get(0).getLong("id"),
                                    stSel.get(0).getLong("reltyp"),
                                    r.getLong("doi"), doi_2, r.getLong("id"), UtCnv.toLong(cl2),
                                    0L, 0L, 0L, 0L)
                        }
                    }
                    if (dimCount == 4 && doi3 > 0 && doi4 == 0) {
                        stSel = mapSel.get(r.getString("id") + "_" + cl2 + "_" + cl3)
                        if (stSel.size() == 1) {
                            saveCubesFace(cubeSFace,
                                    stSel.get(0).getLong("id"),
                                    stSel.get(0).getLong("reltyp"),
                                    r.getLong("doi"), doi_2, r.getLong("id"), UtCnv.toLong(cl2),
                                    doi3, cl3, 0L, 0L)
                        }
                    }
                    if (dimCount == 4 && doi3 > 0 && doi4 > 0) {
                        stSel = mapSel.get(r.getString("id") + "_" + cl2 + "_" + cl3 + "_" + cl4)
                        if (stSel.size() == 1) {
                            saveCubesFace(cubeSFace,
                                    stSel.get(0).getLong("id"),
                                    stSel.get(0).getLong("reltyp"),
                                    r.getLong("doi"), doi_2, r.getLong("id"), UtCnv.toLong(cl2),
                                    doi3, cl3, doi4, cl4)
                        }
                    }

                }
            }
        }
    }

    @DaoMethod
    void deleteProCubeProp(long cube) {
        mdb.execQuery("""
                    with cpc as (
                        select id from CubeSPropCell
                        where cubeS=${cube}
                    )
                    delete from CubeSPropCellItem
                    where cubeSPropCell in (select id from cpc);                    
                    delete from CubeSPropCell
                    where cubeS=${cube};                    
                """)
    }

    @DaoMethod
    void deleteProCubeObj(long cube) {

        mdb.execQuery("""
                    with cf as (
                        select id from CubeSFace
                        where cubeS=${cube}
                    ),
                    cfv as (
                        select id from CubeSFaceValue
                        where cubeSFace in (select id from cf)                    
                    )                    
                    delete from CubeSFaceValueAsgn
                    where cubeSFaceValue in (select id from cfv);                    
                    delete from CubeSFaceValue
                    where id in (
                        select id from CubeSFaceValue
                        except
                        select cubeSFaceValue as id from CubeSFaceValueAsgn 
                    );
                    delete from CubeSFaceDim
                    where cubeSFace in (
                        select id from CubeSFace
                        where cubeS=${cube}                    
                    );
                    delete from CubeSFace
                    where id in (
                        select id from CubeSFace
                        where cubeS=${cube}                    
                    )                    
                """)
    }

}
