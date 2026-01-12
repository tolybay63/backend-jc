package tofi.api.mdl.impl

import jandcode.commons.UtCnv
import jandcode.commons.UtString
import jandcode.commons.variant.VariantMap
import jandcode.core.dbm.mdb.BaseMdbUtils
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.api.mdl.ApiMetaData
import tofi.api.mdl.utils.EntityConst
import tofi.api.mdl.utils.UtMeterSoft
import tofi.apinator.ApinatorApi
import tofi.apinator.ApinatorService
import tofi.api.mdl.model.consts.*;

import java.util.stream.Stream

class ApiMetaDataImpl extends BaseMdbUtils implements ApiMetaData {

    ApinatorApi apiUserData() {
        return app.bean(ApinatorService).getApi("userdata")
    }

    ApinatorApi apiKPIData() {
        return app.bean(ApinatorService).getApi("kpidata")
    }

    ApinatorApi apiPollData() {
        return app.bean(ApinatorService).getApi("polldata")
    }

    ApinatorApi apiIndicatorData() {
        return app.bean(ApinatorService).getApi("indicatordata")
    }


    @Override
    Store loadDataBase() {
        return mdb.loadQuery("""
            select * from DataBase where databaseType=3
        """)
    }

    @Override
    Store loadOwnersParent(Map<String, Object> params) {
        VariantMap pms = new VariantMap(params)
        String propName = "prop"
        if (pms.getString("dataType").equalsIgnoreCase("multi"))
            propName = "multiprop"
        long db = pms.getLong("dataBase")
        boolean isFlat = pms.getString("dataType").equalsIgnoreCase("flat")
        String flat = "cp.flattable is null"
        if (isFlat) flat = "cp.flattable is not null"

        Store stRC = mdb.loadQuery("""
            select distinct cg.relcls, cg.reltyp 
            from RelTypCharGrProp cp left join RelTypCharGr cg on cp.reltypchargr=cg.id and cp.${propName} is not null and ${flat} 
                inner join (select id, reltyp from RelCls where database=${db}) t on cg.reltyp=t.reltyp where 0=0
        """)
        Set<Object> setRC = stRC.getUniqueValues("relcls")
        Set<Object> setRT = stRC.getUniqueValues("reltyp")
        String idsRC = ""
        if (!setRC.isEmpty())
            idsRC = setRC.join(",")
        else {
            stRC = mdb.loadQuery("select id from RelCls where reltyp in (0${setRT.join(",")})")
            idsRC = stRC.getUniqueValues("id").join(",")
        }

        String sql1 = """
            with tt as (
                select distinct cg.typ 
                from TypCharGrProp cp, TypCharGr cg, Cls c 
                where cp.typchargr=cg.id and cp.${propName} is not null and ${flat} and cg.typ=c.typ and c.database=${db}),
        """
        String sql2 = "rr as (select id as relCls from RelCls where id in (0${idsRC}))"

        Store st = mdb.createStore("DomainTreeOwns")
        String sql = sql1 + sql2 + """
                    select 't_'||t.id as id, null as parent, t.cod, v.name, v.fullName, true as isObj, 0 as cls, 0 as relCls,
                        false as loaded, t.id as typORrel, 0 as relTyp
                    from Typ t, TypVer v, tt where t.id=v.ownerVer and v.lastVer=1 and tt.typ=t.id and t.parent is null
                    union all
                    select 't_'||t.id as id, 't_'||t.parent as parent, t.cod, v.name, v.fullName, true as isObj, 0 as cls, 0 as relCls,
                        false as loaded, t.id as typORrel, 0 as relTyp
                    from Typ t, TypVer v, tt where t.id=v.ownerVer and v.lastVer=1 and tt.typ=t.id and t.parent is not null
                    union all
                    select 'r_'||r.id as id, null as parent, r.cod, v.name, v.fullName, false as isObj, 0 as cls, r.id as relCls,
                        false as loaded, r.id as typORrel, r.relTyp
                    from RelCls r
                        left join RelClsVer v on r.id=v.ownerVer and v.lastVer=1
                        where r.id in (select relCls from rr)
                """

        mdb.loadQuery(st, sql)
        //mdb.outTable(st)
        return st
    }

    @Override
    Store loadTypCharGrProp(long tr, boolean isObj, boolean isFlat) {

        Store st = mdb.createStore("TypCharGrProp.prop")
        String sql = """
            with tcg as (
                select distinct id from typchargr where typ=:tr
            )
            select distinct 'p_'||p.id as id,
                case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                p.id as prop, p.propgr, p.propType, p.cod, p.name, flatTable
            from typchargrprop tcp
                left join prop p on p.id=tcp.prop
            where tcp.prop is not null
                and tcp.typchargr in (select id from tcg)
        """
        String tcp = "tcp"
        if (!isObj) {
            st = mdb.createStore("RelTypCharGrProp.prop");

            sql = """
                with rtcg as (
                    select distinct id from reltypchargr where reltyp in (select reltyp from RelCls where id=:tr)
                )
                select distinct
                    'p_'||p.id as id,
                    case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                    p.id as prop, p.propgr, p.propType, p.cod, p.name, flatTable
                from reltypchargrprop rtcp
                    left join prop p on p.id=rtcp.prop
                where rtcp.prop is not null
                    and rtcp.reltypchargr in (select id from rtcg)
            """

            tcp = "rtcp"
        }


        if (isFlat)
            sql = sql + " and ${tcp}.flatTable is not null"
        else
            sql = sql + " and ${tcp}.flatTable is null"

        mdb.loadQuery(st, sql, [tr: tr])
        //
        // PropGr
        Store stGr = mdb.createStore("TypCharGrProp.propGr")

        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr
                    from PropGr where 0=0
                """
        mdb.loadQuery(stGr, sqlGr)
        StoreIndex indStGr = stGr.getIndex("id")

        //setParents for Meter, Factor... without MeterRate
        Set<Object> idsGr = new HashSet<>()
        for (StoreRecord r1 : st) {
            if (r1.getString("parent").startsWith("p_"))
                continue
            String curIdgr = ""
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("propGr") == r2.getLong("propGr")) {
                    curIdgr = r2.getString("id")
                    break
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr)
            idsGr.add(recGr.getString("id").substring(2))
            Object parentGr = recGr.get("parent")
            while (parentGr != "") {
                idsGr.add(UtCnv.toString(parentGr).substring(2))
                recGr = indStGr.get(parentGr)
                parentGr = recGr.get("parent")
            }
        }

        //Parents stGr

        String whe = "(0" + UtString.join(idsGr, ",") + ")"

        stGr = mdb.createStore("TypCharGrProp.prop")
        sqlGr = """
            select 'g_'||id as id, 'g_'||parent as parent, id as propGr, cod, name
            from PropGr where id in 
        """ + whe

        mdb.loadQuery(stGr, sqlGr)
        stGr.add(st)
        //mdb.outTable(stGr)

        Store stGrAll = mdb.createStore("TypCharGrProp.prop")
        String sqlGrAll = """
            select 'g_'||id as id, 'g_'||parent as parent, id as propGr,
            null as prop, null as propType, cod, name, false as checked
            from PropGr where 0=0
            union all
            select 'p_'||p.id as id,
            case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent, p.propgr,
            p.id as prop, p.propType, p.cod, p.name, false as checked
            from Prop p
            where 0=0
        """
        mdb.loadQuery(stGrAll, sqlGrAll)
        StoreIndex indStAll = stGrAll.getIndex("id")
        //mdb.outTable(stGrAll)

        //Analize parents
        Set<Object> ids = stGr.getUniqueValues("id")
        Set<Object> setPropGr = new HashSet<>()
        Set<Object> setProp = new HashSet<>()
        for (StoreRecord r : stGr) {
            if (r.getString("parent").isBlank())
                continue
            if (!ids.contains(r.getString("parent"))) {
                setPropGr.add(r.getLong("propGr"))
                setProp.add(r.getLong("prop"))
            }
        }

        if (setProp.isEmpty()) {
            Stream<StoreRecord> strm = stGr.getRecords().stream().filter(r -> r.getLong("propType") == FD_PropType_consts.complex)
            List<String> lst = new ArrayList<>()
            List<String> lstTmp1 = new ArrayList<>()
            strm.forEach(s -> lst.add(s.getString("id")))
            //
            stGr.getRecords().forEach(r -> {
                if (lst.contains(r.getString("parent"))) {
                    r.set("isItem", true)
                    lstTmp1.add(r.getString("id"))
                }
            })
            // ToDo
            List<String> lstTmp2 = new ArrayList<>();
            stGr.getRecords().forEach(r -> {
                if (lstTmp1.contains(r.getString("parent"))) {
                    r.set("isItem", true)
                    lstTmp2.add(r.getString("id"))
                }
            })
            List<String> lstTmp3 = new ArrayList<>()
            stGr.getRecords().forEach(r -> {
                if (lstTmp2.contains(r.getString("parent"))) {
                    r.set("isItem", true)
                    lstTmp3.add(r.getString("id"))
                }
            })
            //Last 4 lev
            stGr.getRecords().forEach(r -> {
                if (lstTmp3.contains(r.getString("parent"))) {
                    r.set("isItem", true)
                }
            })
            //
            return stGr
        }
        //
        String whGr = "(" + UtString.join(setPropGr, ",") + ")"
        if (whGr.equals("()")) whGr = "(0)"

        Store stPath = mdb.createStore("DomainPath")
        mdb.loadQuery(stPath, "select id, parent, '' as path from Prop where propGr in " + whGr)
        UtMeterSoft utMeterSoft = new UtMeterSoft(mdb, 0, false)
        utMeterSoft.setPath(stPath)
        //mdb.outTable(stPath)
        StoreIndex indStProp = stPath.getIndex("id")
        StoreIndex indStGrProp = stGr.getIndex("id")

        Store stGrCopy = mdb.createStore("TypCharGrProp.prop")
        Set<Object> idsPropGrDop = new HashSet<>()

        for (Object it : setPropGr) {
            String gr = "g_" + UtCnv.toString(it)
            for (StoreRecord r : stGr) {
                if (!setProp.contains(r.getLong("prop"))) continue
                StoreRecord rec = indStProp.get(r.getLong("prop"))
                List<String> lstPrts = List.of(rec.getString("path").split(","))
                for (String el : lstPrts) {
                    boolean ok = false
                    if (ids.contains("p_" + el)) {
                        //StoreRecord recGr = indStGrProp.get(UtCnv.toLong(el));
                        r.set("parent", "p_" + el)
                        ok = true
                        break
                    }
                    if (!ok) {
                        StoreRecord recGr = indStGrProp.get(gr)
                        if (recGr != null)
                            r.set("parent", recGr.getString("parent"))
                        else {
                            recGr = indStAll.get(gr)
                            while (true) {
                                if (recGr.getString("id").startsWith("g_"))
                                    break
                                recGr = indStAll.get(recGr.getString("parent"))
                            }
                            r.set("parent", recGr.getString("parent"))
                            idsPropGrDop.add(recGr.getLong("propGr"))
                        }
                    }
                }
            }
        }

        mdb.loadQuery(stGrCopy, sqlGr);
        //
        stGr.add(stGrCopy);

        //mdb.outTable(stGrCopy);
        //mdb.outTable(stGr);

        return stGr;
    }

    @Override
    Store loadCharGrMultiProp(long tr, boolean isObj) {
        Store st = mdb.createStore("Prop.multi")
        mdb.loadQuery(st, """
            select 'ch_'||id as id, cod, null as parent, name, 0 as isitem,
                null as multiProp, null as dimMultiProp, null as dimMultiPropItem
            from TypCharGr
            where typ=1010 and id in (
                select tcp.typchargr
                from TypCharGrProp tcp
                    inner join MultiProp mp on tcp.multiprop=mp.id
                    left join MultiPropDim mpd on mp.id=mpd.multiprop
                where typchargr in (
                    select id from TypCharGr where typ=:tr
                )
            )
            union all
            select 'mp_'||mp.id as id, mp.cod, 'ch_'||tcp.typchargr as parent, mp.name, 0 as isitem,
                mp.id as multiProp, mpd.dimMultiProp, null as dimMultiPropItem
            from TypCharGrProp tcp
                inner join MultiProp mp on tcp.multiprop=mp.id
                left join MultiPropDim mpd on mp.id=mpd.multiprop
            where typchargr in (
                select id from TypCharGr where typ=:tr
            )
        """, [tr: tr])


        Store stAll = mdb.createStore("Prop.multi")
        stAll.add(st)

        for (StoreRecord r : st) {
            if (r.getLong("dimmultiprop")==0) continue
            Store stTmp = mdb.createStore("Prop.multi")
            mdb.loadQuery(stTmp,"""
                select 'mpi_'||dmpi.id as id, dmpi.cod, 'mpi_'||dmpi.parent as parent, dmpi.name,  1 as isitem, 
                     mpd.multiProp, dmpi.dimMultiProp, dmpi.id as dimMultiPropItem
                from DimMultiPropItem dmpi
                    left join DimMultiProp dmp on dmp.id=dmpi.dimmultiprop 
                    left join MultiPropDim mpd on mpd.dimmultiprop=dmp.id
                where dmpi.dimmultiprop=:dmp
                order by 'pi_'||dmpi.parent desc, 'pi_'||dmpi.id
            """, [dmp: r.getLong("dimmultiprop")])

            for (StoreRecord rr : stTmp) {
                if (rr.getString("parent")=="") {
                    rr.set("parent", r.getString("id"))
                }
            }
            stAll.add(stTmp)
        }

        mdb.outTable(stAll)
        return stAll
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
    Store loadPropValEntityForSelect(long prop, long entityType) {
        Store st = mdb.createStore("Prop.entity.select")

        EntityConst.EntityInfo ei = EntityConst.getEntityInfo(entityType)
        String tabl = ei.getTableName()
        String wheFV = ""
        if (ei.getSign().equalsIgnoreCase("FactorVal")) {
            wheFV = " and t.parent is not null"
        }

        String sql = """
            select p.id, t.cod, t.name, t.fullName
            from PropVal p left join ${tabl} t on p.entityId=t.id where prop=:p
        """

        if (ei.getHasVer()) {
            sql = """
                select p.id, t.cod, v.name, v.fullName
                from PropVal p left join ${tabl} t on p.entityId=t.id left join
                    ${tabl}Ver v on t.id=v.ownerVer and v.lastVer=1 where prop=:p ${wheFV}
            """
        }
        mdb.loadQuery(st, sql, [p: prop])
        return st
    }

    @Override
    Map<Long, Long> mapIdEntityFromPV(String entity, long prop) {
        Map<Long, Long> res = [:]

        Store st = mdb.loadQuery("""
                select id, ${entity} from PropVal where ${entity} is not null and prop=${prop}
            """)
        for (StoreRecord r in st) {
            res.put(r.getLong("id"), r.getLong(entity))
        }
        return res
    }

}
