package tofi.mdl.model.dao.meter

import jandcode.commons.UtCnv
import jandcode.core.dbm.mdb.Mdb
import jandcode.core.store.Store
import jandcode.core.store.StoreIndex
import jandcode.core.store.StoreRecord
import tofi.mdl.model.utils.EntityMdbUtils
import tofi.mdl.model.utils.UtEntityTranslate
import tofi.mdl.model.utils.UtMeterSoft

import java.util.stream.Stream

class MeterRateMdbUtils extends EntityMdbUtils {
    Mdb mdb
    String tableName

    MeterRateMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName)
        this.mdb = mdb
        this.tableName = tableName
    }

    Store loadSoftMR(long meter, String lang) throws Exception {
        UtMeterSoft ut = new UtMeterSoft(mdb, meter, lang)
        Store st = ut.getMeterRatesWithParent()
        return st
    }

    void createAllSoftMR(long meter) throws Exception {
        UtMeterSoft ut = new UtMeterSoft(mdb, meter, "ru", true)
        ut.createMeterRates(null)
    }

    void deleteSoftMR(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"))
        long mr = UtCnv.toLong(rec.get("id"))
        try {
            mdb.execQuery("""
                delete from MeterRateFV where meterrate=:mr
            """, Map.of("mr", mr))
        } finally {
            deleteEntity(rec)
        }
    }

    void deleteAllMR(long meter) throws Exception {
        try {
            mdb.execQuery("""
                delete from MeterRateFV where meterrate in (
                    select id from MeterRate where meter=:meter
                );
                delete from MeterRate where meter=:meter;                
            """, Map.of("meter", meter))
        } finally {
            mdb.execQuery("""
                delete from syscod where linkid in (
                    select linkid as id 
                    from syscod
                    where linktype=6
                    except 
                    select id from meterrate  
                )              
            """)
        }
    }

    void updateSoftMR(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"))
        updateEntity(rec)
    }


    //**********************************************************
    Store loadHardMR(long meter, String lang) throws Exception {
        Store st = mdb.createStore("MeterRate.lang")
        mdb.loadQuery(st, """
            select * from MeterRate where meter=:meter order by parent,ord
        """, Map.of("meter", meter))
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"MeterRate", lang);
    }

    void insertHardMR(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"))
        insertEntity(rec)
    }

    void updateHardMR(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"))
        updateEntity(rec)
    }

    void deleteHardMR(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"))
        deleteEntity(rec)
    }
    //

    Store loadMeterSoftForUpd(long meter, String lang) throws Exception {
        UtMeterSoft utMS = new UtMeterSoft(mdb, meter, lang, true)
        Store stAll = utMS.generateAllMeterRates()
        Store st = utMS.getMeterRatesWithParent()

        //mdb.outTable(st)

        for (StoreRecord r1 : st) {
            Stream<StoreRecord> fltAll = stAll.getRecords().stream().filter(r -> {
                return r.getInt("sz") == r1.getInt("sz")
            })
            List<String> lst1 = UtCnv.toList(r1.getString("fvs"))
            Stream<StoreRecord> fltAllChecked = fltAll.filter(r -> {
                List<String> lst2 = UtCnv.toList(r.getString("fvs"))
                return lst1.containsAll(lst2) && lst2.containsAll(lst1)
            })
            fltAllChecked.forEach(r -> {
                r.set("checked", true)
                r.set("cod", r1.getString("cod"))
            })
        }

        //mdb.outTable(stAll)

        return stAll;
    }

    Store loadMeterSoftForUpdSave(Map<String, Object> params) throws Exception {
        long meter = UtCnv.toLong(params.get("meter"));
        String lang = UtCnv.toString(params.get("lang"));
        List<Map<String, Object>> lstCheckeds = (List<Map<String, Object>>) params.get("checkeds");

        UtMeterSoft utMeterSoft = new UtMeterSoft(mdb, meter, lang, false);
        Store stPath = utMeterSoft.generateAllMeterRates();
        utMeterSoft.setPath(stPath);

        //mdb.outTable(stPath);

        Store stRez = mdb.createStore("MeterRate.soft.tree");

        Map<Long, Long> mapIdParent = new HashMap<>();
        for (Map<String, Object> map : lstCheckeds) {
            stRez.add(map);
            mapIdParent.put(UtCnv.toLong(map.get("id")), UtCnv.toLong(map.get("parent")));
        }

        StoreIndex indStPath = stPath.getIndex("id");

        for (long id : mapIdParent.keySet()) {
            StoreRecord r = indStPath.get(id);
            if (!mapIdParent.containsKey(r.getLong("parent"))) {
                if (r.getString("path").contains(",")) {
                    List<Long> lstPath = utMeterSoft.strToList((r.getString("path")));
                    for (Long aLong : lstPath) {
                        boolean ok = false;
                        if (mapIdParent.containsKey(aLong)) {
                            r.set("parent", aLong);
                            ok = true;
                            break;
                        }
                        r.set("parent", null);
                    }
                } else {
                    long prt = r.getLong("path");
                    if (prt > 0) {
                        if (!mapIdParent.containsKey(prt))
                            r.set("parent", null);
                    }
                }
            }
        }

        for (StoreRecord r : stRez) {
            StoreRecord record = indStPath.get(r.getLong("id"));
            r.set("parent", record.get("parent"));
        }

        return stRez;
    }

    void saveMeterSoftRates(Map<String, Object> params) throws Exception {
        long meter = UtCnv.toLong(params.get("meter"))
        String lang = UtCnv.toString(params.get("lang"))
        UtMeterSoft utMS = new UtMeterSoft(mdb, meter, lang, true)
        List<Map<String, Object>> lstData = (List<Map<String, Object>>) params.get("data")

        //Old meterrates
        Store stOld = mdb.loadQuery("select id, cod from MeterRate where meter=:m", Map.of("m", meter))
        Set<String> setOldMR = stOld.getUniqueValues("cod") as Set<String>

        Set<String> setNewMR = new LinkedHashSet<>();
        Store st = mdb.createStore("MeterRate.soft.tree")
        for (Map<String, Object> map : lstData) {
            map.put("id", null)
            map.put("parent", null)
            st.add(map)
            setNewMR.add(UtCnv.toString(map.get("cod")));
        }
        //mdb.outTable(st)

        //Deleting
        stOld.forEach(r -> {
            if (!setNewMR.contains(r.getString("cod"))) {
                String sql = "delete from MeterRateFV where meterRate=:mr" + ";" +
                        "delete from MeterRate where id=:mr" + ";" +
                        "delete from TableLang where nameTable='MeterRate' and idTable=:mr" + ";"
                mdb.execQuery(sql, Map.of("mr", r.getLong("id")));
            }
        })

        st.forEach(r -> {
            if (!setOldMR.contains(r.getString("cod"))) {
                List<Object> lstFv = utMS.strToList(r.getString("fvs"))
                Map<String, Object> recLang = r.getValues()
                recLang.put("lang", lang)
                long idMR = insertEntity(recLang)
                lstFv.forEach(l -> {
                    mdb.insertRec("MeterRateFV", Map.of("meterRate", idMR, "factorVal", UtCnv.toLong(l)))
                })
            }
        })

    }


}
