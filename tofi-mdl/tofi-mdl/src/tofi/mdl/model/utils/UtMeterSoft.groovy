package tofi.mdl.model.utils

import jandcode.commons.UtCnv
import jandcode.commons.UtLang
import jandcode.commons.error.XError
import jandcode.core.dbm.domain.Domain
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreRecord
import tofi.mdl.consts.FD_MeterStruct_consts
import tofi.mdl.model.dao.meter.MeterMdbUtils
import tofi.mdl.model.dao.meter.MeterRateMdbUtils

import java.text.MessageFormat

class UtMeterSoft {
    Mdb mdb
    String domainResult = "MeterRate.soft.tree"
    long meter = 0
    String lang
    boolean withNameMeter = true

    UtMeterSoft(Mdb mdb, long meter, String lang, boolean withNameMeter = true) {
        this.mdb = mdb
        this.meter = meter
        this.lang = lang
        this.withNameMeter = withNameMeter
    }

    Store getMeterRatesWithParent() throws Exception {
        String sMeter = null

        int sz = mdb.loadQuery("select count(*) as cnt from MeterFactor where meter=:m",
                Map.of("m", meter)).get(0).getInt("cnt")

        Store ds = mdb.loadQuery("""
            with fv as (
            select meterrate,
                string_agg (cast(m.factorval as varchar(4000)), ',' order by mf.orddim, mf.ordfactorInDim) as fvs,
                array_length(string_to_array(string_agg (cast(m.factorval as varchar(4000)), ','), ','), 1) sz
            from meterratefv m 
                Left Join factor f on m.factorval=f.id
                left join meterrate mr on mr.id=m.meterrate
                left join factor fv on fv.id=m.factorval 
                left join meterfactor mf on mf.meter=:meter and mf.factor =fv.id and fv.parent is null
            where mr.meter=:meter 
            group by m.meterrate
            )
            select mr.*, fv.fvs, fv.sz, '' as name, '' as fullName, '' as cmt
            from fv
            left join meterrate mr on mr.id=fv.meterrate
        """, [meter: meter])

        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb)
        ds = ut.getTranslatedStore(ds,"MeterRate", lang)
        //

        Store dsRes = mdb.createStore(domainResult)
        //
        for (int i = 1; i < sz + 1; i++) {
            for (StoreRecord r : ds) {
                if (r.getInt("sz") == i) {
                    Store st1 = mdb.createStore(domainResult)
                    StoreRecord r1 = st1.add(r)
                    if (r.getInt("sz") == 1) {
                        r1.set("parent", sMeter)
                        dsRes.add(r1)
                    } else {
                        StoreRecord record = toRes(dsRes, r1, i - 1, sMeter)
                        dsRes.add(record)
                    }
                }
            }
        }
        return dsRes
    }

    protected static StoreRecord toRes(Store dsRes, StoreRecord rec, int lev, String sMeter) {
        for (int l = lev; l > 0; l--) {
            for (StoreRecord r in dsRes) {
                if (r.getInt("sz") == lev) {
                    List<Long> arr1 = rec.getString("fvs").split(",") as List<Long>
                    List<Long> arr0 = r.getString("fvs").split(",") as List<Long>
                    if (arr1.containsAll(arr0)) {
                        rec.set("parent", r.getLong("id"))
                        return rec
                    }
                }
            }
        }
        rec.set("parent", sMeter)
        return rec
    }
    //

    Store generateAllMeterRates() throws Exception {
        MeterMdbUtils mdbUtils = new MeterMdbUtils(mdb, "Meter")
        StoreRecord rM = mdbUtils.loadRec(Map.of("id", meter, "lang", lang)).get(0)
        if (rM.getLong("meterstruct") != FD_MeterStruct_consts.soft) {
            throw new XError(MessageFormat.format(UtLang.t("Измеритель с кодом [{0}] не является мягкой"), rM.getString("cod")))
        }

        String sqlMF = """
            select * from MeterFactor where meter=:m
            order by ordDim, ordFactorInDim
        """
        Store dsMF = mdb.loadQuery(sqlMF, Map.of("m", meter))
        if (dsMF.size() == 0) {
            throw new XError(MessageFormat.format(UtLang.t("Для измерителя с кодом [{0}] не указаны факторы"), rM.getString("cod")))
        }
        int sz = dsMF.size()
        int szDim = dsMF.getUniqueValues("ordDim").size()
        Store dsRes = mdb.createStore("MeterRate.soft.tree")
        //

        int cnt = sz
        long idInc = 1
        for (int i=0; i < cnt; i++) {
            Store dsStep = generateMeterRates(rM, dsMF, sz, szDim, idInc)
            dsRes.add(dsStep)
            dsMF.getRecords().remove(0)
            int od = 1
            dsMF.forEach {StoreRecord it ->
                it.set("orddim", od++)
            }
            sz--
            szDim--
            idInc = idInc + 1000
        }
        return dsRes
    }

    Store generateMeterRates(StoreRecord rM, Store dsMF, int sz, int szDim, long idInc) throws Exception {
        List<List<StoreRecord>> lstlstFV = new ArrayList<List<StoreRecord>>()
        //
        String sMeter = null
        for (int l = 1; l < szDim + 1; l++) {
            List<StoreRecord> dsMFfilter = dsMF.records.stream().findAll({ rr ->
                rr.getInt("ordDim") == l
            }).collect()

            Store dsFV = mdb.createStore("Factor.lang")
            //
            if (l > 1) {
                mdb.loadQuery(dsFV, """
                    select 0 as id,'' as name, '' as fullname, 0 as parent, 0 as ord
                """)
            }
            //
            for (StoreRecord rr : dsMFfilter) {
                long factor = rr.getLong("factor")
                mdb.loadQuery(dsFV, """
                    select id,coalesce(parent, 0) as parent, ord 
                    from Factor 
                    where parent is not null and parent=:f 
                    order by ord
                """, [f: factor])
            }
            //
            UtEntityTranslate ut = new UtEntityTranslate(mdb);
            dsFV = ut.getTranslatedStore(dsFV,"Factor", lang);
            //
            List<StoreRecord> lstFV = dsFV.getRecords()
            lstlstFV.add(lstFV)
        }
        List<List<StoreRecord>> res = CartesianProduct.result(lstlstFV)
        Store st = mdb.createStore("MeterRate.soft.tree")

        for (List<StoreRecord> lst in res) {
            if (isCompatible(lst)) {
                def nmArr = []; def fnArr = []
/*
                System.out.println("idInc: "+idInc)
                System.out.println("size: "+lst.size())
                lst.forEach {StoreRecord it->
                    mdb.outTable(it)
                }
*/

                lst.each { r ->
                    //mdb.outTable(r)
                    if (!r.getString("name").isBlank()) {
                        nmArr.add(r.getString("name"))
                    }
                    if (!r.getString("fullName").isBlank()) {
                        fnArr.add(r.getString("fullName"))
                    }
                }
                //
                String nm = null, fn = null

                if (nmArr.size() > 0) {
                    nm = (withNameMeter) ? rM.getString("name") : ""
                    nm = nm + " (" + nmArr.join("; ") + ")"
                }

                if (fnArr.size() > 0) {
                    fn = (withNameMeter) ? rM.getString("fullName") : ""
                    fn = fn + " (" + fnArr.join("; ") + ")"
                }
                //
                if ((nm && !nm.isBlank())) {
                    StoreRecord rMR = st.add()
                    rMR.setValue("id", idInc++)
                    rMR.setValue("accessLevel", 1L)
                    rMR.setValue("meter", meter)
                    rMR.setValue("name", nm)
                    rMR.setValue("fullName", fn)
                    //
                    Set<Object> setFv = new HashSet<>()
                    lst.each { r ->
                        if (r.getLong("id") != 0) {
                            setFv.add(r.getLong("id"))
                        }
                    }
                    String sFv = String.join(",", UtCnv.toString(setFv)).
                            replace("[", "").replace("]", "").replace(" ", "")
                    rMR.setValue("fvs", sFv)
                    rMR.setValue("sz", setFv.size())
                }
            }
        }
        //
        Store dsRes = mdb.createStore("MeterRate.soft.tree")
        for (int i = 1; i < sz + 1; i++) {
            for (StoreRecord r : st) {
                if (r.getInt("sz") == i) {
                    Store st1 = mdb.createStore(domainResult)
                    StoreRecord r1 = st1.add(r)
                    if (r.getInt("sz") == 1) {
                        r1.set("parent", sMeter)
                        dsRes.add(r1)
                    } else {
                        StoreRecord record = toRes(dsRes, r1, i - 1, sMeter)
                        dsRes.add(record)
                    }
                }
            }
        }
        return dsRes
    }

    /**
     * Создание всех показателей мягкого измерителя с учетом зависимости значений факторов каждого измерения
     * @param meter
     * @param lstFVs
     * @throws Exception
     */
    void createMeterRates(List<Long> lstFVs) throws Exception {
        MeterMdbUtils mdbUtils = new MeterMdbUtils(mdb, "Meter")
        StoreRecord rM = mdbUtils.loadRec(Map.of("id", meter)).get(0)
        if (rM.getLong("meterstruct") != FD_MeterStruct_consts.soft) {
            throw new XError(MessageFormat.format(UtLang.t("Измеритель с кодом [{0}] не является мягкой"), rM.getString("cod")))
        }
        List<List<StoreRecord>> lstlstFV = new ArrayList<List<StoreRecord>>()
        String sqlMF = """
            select * from MeterFactor where meter=:m
            order by ordDim, ordFactorInDim
        """
        Store dsMF = mdb.loadQuery(sqlMF, Map.of("m", meter))
        if (dsMF.size() == 0) {
            throw new XError(MessageFormat.format(UtLang.t("Для измерителя с кодом [{0}] не указаны факторы"), rM.getString("cod")))
        }
        //
        int szDim = dsMF.getUniqueValues("ordDim").size()
        for (int l = 1; l < szDim + 1; l++) {
            List<StoreRecord> dsMFfilter = dsMF.records.stream().findAll({ rr ->
                rr.getInt("ordDim") == l
            }).collect()

            Store dsFV = mdb.createStore("Factor")
            //
            if (l > 1) {
                mdb.loadQuery(dsFV, """
                    select 0 as id,'' as name, '' as fullname, 0 as parent, 0 as ord
                """)
            }
            //
            for (StoreRecord rr : dsMFfilter) {
                long factor = rr.getLong("factor")
                mdb.loadQuery(dsFV, """
                    select id,name,fullName,coalesce(parent, 0) as parent, ord 
                    from Factor where parent is not null and parent=:f order by ord
                """, [f: factor])
            }

            List<StoreRecord> lstFV = dsFV.getRecords()
            lstlstFV.add(lstFV)
        }
        List<List<StoreRecord>> res = CartesianProduct.result(lstlstFV)
        MeterRateMdbUtils mdbMR = new MeterRateMdbUtils(mdb, "MeterRate")
        //
        for (List<StoreRecord> lst in res) {
            if (isCompatible(lst)) {
                def nmArr = []; def fnArr = []
                boolean insOk = true
                //System.out.println("size: "+lst.size())
                lst.each { r ->
                    //mdb.outTable(r)
                    if (!r.getString("name").isBlank()) {
                        nmArr.add(r.getString("name"))
                    }
                    if (!r.getString("fullName").isBlank()) {
                        fnArr.add(r.getString("fullName"))
                    }
                    if (lstFVs != null && lstFVs.size() > 0) {
                        if (r.getLong("id") != 0) {
                            if (!lstFVs.contains(r.getLong("id")))
                                insOk = false
                        }
                    }
                }
                //
                if (insOk) {
                    String nm = null, fn = null

                    if (nmArr.size() > 0) {
                        nm = (withNameMeter) ? rM.getString("name") : ""
                        nm = nm + " (" + nmArr.join("; ") + ")"
                    }

                    if (fnArr.size() > 0) {
                        fn = (withNameMeter) ? rM.getString("fullName") : ""
                        fn = fn + " (" + fnArr.join("; ") + ")"
                    }
                    //
                    if ((nm && !nm.isBlank())) {
                        Store dsMR = mdb.createStore("MeterRate")
                        StoreRecord rMR = dsMR.add()
                        rMR.setValue("accessLevel", 1L)
                        rMR.setValue("meter", meter)
                        rMR.setValue("name", nm)
                        rMR.setValue("fullName", fn)
                        long idMR = mdbMR.insertEntity(rMR.getValues())
                        //
                        lst.each { r ->
                            if (r.getLong("id") != 0) {
                                mdb.insertRec("MeterRateFV", [meterRate: idMR, factorVal: r.getLong("id")], true)
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Определяет совместность lst
     * @param lst
     * @return
     * @throws Exception
     */
    protected boolean isCompatible(List<StoreRecord> lst) throws Exception {
        //
        List<Long> fvs = new ArrayList<Long>()
        lst.each({ r ->
            fvs.add(r.getLong("id"))
        })
        //int sz = fvs.size()
        String whFV = fvs.join(",")
        //
        Store dsFVrel = mdb.loadQuery("""
          select * from FactorValRel where factor1 in (0${whFV}) and factor2 in (0${whFV})
        """)

        return dsFVrel.size() <= 0

    }


    void setPath(Store st) throws Exception {
        Domain dn = mdb.createDomain(st)
        Store stCpy = mdb.createStore(dn)
        st.copyTo(stCpy)
        for (StoreRecord r : st) {
            List<Long> lstParent = new ArrayList<>()
            if (r.getLong("parent") > 0) {
                long prt = r.getLong("parent")
                while (true) {
                    StoreRecord rP = getParentRecord(stCpy, prt)
                    if (rP == null) {
                        throw new XError("Error!")
                    }
                    lstParent.add(rP.getLong("id"))
                    prt = rP.getLong("parent")
                    if (rP.getLong("parent") == 0) break
                }
            } else {
                lstParent.add(0L)
            }
            r.set("path", String.join(",", UtCnv.toString(lstParent)
                    .replace("[", "").replace("]", "").replace(" ", "")))
        }
    }

    protected static StoreRecord getParentRecord(Store st, long recordParent) {
        Map<Long, StoreRecord> map = new HashMap<>()
        for (StoreRecord r : st) {
            if (r.getLong("id") == recordParent) {
                map.put(recordParent, r)
                break
            }
        }
        return map.get(recordParent)
    }

    static List<Long> strToList(String str) throws Exception {
        List<Long> lst = new ArrayList<Long>()
        String[] arr = str.split(",")
        for (String s : arr)
            lst.add(UtCnv.toLong(s))
        return lst
    }

}
