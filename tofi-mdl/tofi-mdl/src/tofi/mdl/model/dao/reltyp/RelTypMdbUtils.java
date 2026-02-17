package tofi.mdl.model.dao.reltyp;

import jandcode.commons.UtCnv;
import jandcode.commons.UtJson;
import jandcode.commons.error.XError;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.dbm.sql.SqlText;
import jandcode.core.store.Store;
import jandcode.core.store.StoreIndex;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_PropType_consts;
import tofi.mdl.consts.FD_StorageType_consts;
import tofi.mdl.model.utils.EntityMdbUtils;
import tofi.mdl.model.utils.UtEntityTranslate;
import tofi.mdl.model.utils.UtMeterSoft;

import java.util.*;
import java.util.stream.Stream;

public class RelTypMdbUtils extends EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public RelTypMdbUtils(Mdb mdb, String tableName) throws Exception {
        super(mdb, tableName);
        this.mdb = mdb;
        this.tableName = tableName;
    }

    //---------------------------------------------------

    public Map<String, Object> loadRelTypPaginate(Map<String, Object> params) throws Exception {
        String lang = UtCnv.toString(params.get("lang"));
        //count
        String sqlCount = """
            select count(*) as cnt
            from RelTyp t
                left join RelTypVer v on t.id=v.ownerVer and v.lastVer=1
                left join TableLang l on l.nameTable='RelTypVer' and l.idTable=v.id and l.lang=:lang
            where 0=0
        """;
        SqlText sqlText = mdb.createSqlText(sqlCount);
        sqlText.setSql(sqlCount);
        String filter = UtCnv.toString(params.get("filter")).trim();
        String textFilter = "name like '%" + filter + "%' or fullName like '%" + filter + "%' or cod like '%" + filter + "%' or cmt like '%" + filter + "%'";
        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        int total = mdb.loadQuery(sqlText, Map.of("lang", lang)).get(0).getInt("cnt");
        int lm = UtCnv.toInt(params.get("rowsPerPage")) == 0 ? total : UtCnv.toInt(params.get("rowsPerPage"));
        int offset = (UtCnv.toInt(params.get("page")) - 1) * lm;
        Map<String, Object> meta = new HashMap<>();
        meta.put("total", total);
        meta.put("page", UtCnv.toInt(params.get("page")));
        meta.put("limit", lm);
        // load
        String sqlLoad = """
            select *, v.id as verId
            from RelTyp t
                left join RelTypVer v on t.id=v.ownerVer and v.lastVer=1
                left join TableLang l on l.nameTable='RelTypVer' and l.idTable=v.id and l.lang=:lang
            where 0=0
        """;
        sqlText = mdb.createSqlText(sqlLoad);
        sqlText.paginate(true);

        if (!UtCnv.toString(params.get("sortBy")).trim().isEmpty()) {
            String orderBy = UtCnv.toString(params.get("sortBy"));
            if (UtCnv.toBoolean(params.get("descending"))) {
                orderBy = orderBy + " desc";
            }
            sqlText = sqlText.replaceOrderBy(orderBy);
        }

        if (!filter.isEmpty())
            sqlText = sqlText.addWhere(textFilter);
        Store stLoad = mdb.createStore("RelTyp.lang");
        mdb.loadQuery(stLoad, sqlText, Map.of("lang", lang, "offset", offset, "limit", lm));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        stLoad = ut.getTranslatedStore(stLoad,"RelTyp", lang, true);
        //
        return Map.of("store", stLoad, "meta", meta);
    }
    //

    public void delete(Map<String, Object> rec) throws Exception {
        deleteEntity(rec);
    }

    /**
     * Update Type
     *
     * @param params Map
     * @return Store
     */
    public Store update(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(params);
        //
        return loadRec(params);    }

    public Store insert(Map<String, Object> params) throws Exception {
        long id = insertEntity(params);
        params.put("id", id);
        return loadRec(params);
    }

    public Store loadRec(Map<String, Object> params) throws Exception {
        long id = UtCnv.toLong(params.get("id"));
        Store st = mdb.createStore("RelTyp.lang");
        mdb.loadQuery(st, """
            select *, v.id as verId from RelTyp t, RelTypVer v where t.id=v.ownerVer and v.lastVer=1 and t.id=:id
        """, Map.of("id", id));

        //mdb.outTable(st);

        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        String lang = UtCnv.toString(params.get("lang"));
        return ut.getTranslatedStore(st,"RelTyp", lang, true);
    }


    //---------------------- RelTypVer -------------------------------

    public Store loadVer(long reltyp, String lang) throws Exception {
        Store st = mdb.createStore("RelTyp.lang");
        mdb.loadQuery(st, """
            select *, v.id as verId
            from RelTyp t
                left Join RelTypVer v on t.id=v.ownerVer
                left join TableLang l on l.nameTable='RelTypVer' and l.idTable=v.id and l.lang=:lang
            where v.ownerVer=:reltyp
            order by dend desc
         """, Map.of("reltyp", reltyp, "lang", lang));
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelTyp", lang, true);

    }

    public Store loadVerRec(long id, String lang) throws Exception {
        Store st = mdb.createStore("RelTyp.lang");
        mdb.loadQuery(st, """
            select *, v.id as verId
            from RelTypVer v
                left join TableLang l on l.nameTable='RelTypVer' and l.idTable=v.id and l.lang=:lang
            where v.id=:id
         """, Map.of("id", id, "lang", lang));
        //
        UtEntityTranslate ut = new UtEntityTranslate(mdb);
        return ut.getTranslatedStore(st,"RelTyp", lang, true);
    }

    public void deleteVer(Map<String, Object> rec) throws Exception {
        deleteEntityVer(rec);
    }

    public Store insertVer(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = UtCnv.toMap(params.get("rec"));
        //
        long id = insertEntityVer(rec);
        //
        return loadVerRec(id, UtCnv.toString(rec.get("lang")));
    }

    public Store updateVer(Map<String, Object> params) throws Exception {
        Map<String, Object> rec = (UtCnv.toMap(params.get("rec")));
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        //
        updateEntityVer(rec);
        // Загрузка записи
        return loadVerRec(id, UtCnv.toString(rec.get("lang")));
    }

    //loadRelClsForSelect
    public Store loadRelClsForSelect(long relTyp, String lang) throws Exception {
        return mdb.loadQuery("""
            select r.id, l.name
            from RelCls r
                left join RelClsVer v on r.id=v.ownerVer and v.lastVer=1
                left join TableLang l on l.nameTable='RelClsVer' and l.idTable=v.id and l.lang=:lang
            where r.relTyp=:rt
        """, Map.of("rt", relTyp, lang, lang));
    }

    //RelTypCharGr
    public Store loadRelTypCharGr(long id, String lang) throws Exception {
        String whe = " 0=0 order by r.ord";
        if (id > 0)
            whe = " r.id=" + id;

        Store st = mdb.createStore("RelTypCharGr.lang");
        mdb.loadQuery(st, """
            select r.*, lrc.name as relClsName, lr.cmt, lb.name as dbNames, d.modelname, d.id as dbId
            from RelTypCharGr r
                left join TableLang lr on lr.nameTable='RelTypCharGr' and lr.idTable=r.id and lr.lang=:lang
                left join RelCls c on c.id=r.relcls
                left join RelClsVer v on c.id=v.ownerVer and v.lastVer=1
                left join TableLang lrc on lrc.nameTable='RelClsVer' and lrc.idTable=v.id and lrc.lang=:lang
                left join database d on d.id=c."database"
                left join TableLang lb on lb.nameTable='DataBase' and lb.idTable=d.id and lb.lang=:lang
            where
        """+whe, Map.of("lang", lang));

        Store stDB = mdb.loadQuery("""
            select distinct r.reltyp, modelname, lb.name
            from RelCls r
                inner join database d on r."database"=d.id
                left join TableLang lb on lb.nameTable='DataBase' and idTable=d.id and lb.lang=:lang
            where 0=0
        """, Map.of("lang", lang));
        StoreIndex indDB = stDB.getIndex("reltyp");
        for (StoreRecord r : st) {
            if (r.isValueNull("modelname")) {
                StoreRecord rec = indDB.get(r.getLong("reltyp"));
                if (rec != null) {
                    r.set("modelname", rec.getString("modelname"));
                    r.set("dbNames", rec.getString("name"));
                    r.set("relClsName", "Все классы");
                }
            }
        }

        return st;
    }

    protected Store loadRelTypCharGrRec(long id, String lang) throws Exception {
        return loadRelTypCharGr(id, lang);
    }

    public StoreRecord loadRelTypCharGrInfo(long id, String lang) throws Exception {
        StoreRecord st = mdb.createStoreRecord("RelTypCharGr.info");
        mdb.loadQueryRecord(st, """
            select r.id, r.cod, r.name as rcgName, d.modelName, d.id as dbs,d.name as dbTitle
            from RelTypCharGr r
                left join RelCls c on c.id=r.relcls
                left join RelClsVer v on c.id=v.ownerVer and v.lastVer=1
                left join TableLang lr on lr.nameTable='RelClsVer' and lr.idTable=v.id and lr.lang=:lang
                left join database d on d.id=c."database"
                left join TableLang lb on lb.nameTable='DataBase' and lb.idTable=d.id and lb.lang=:lang
            where r.id=:id
        """, Map.of("id", id, "lang", lang));
        return st;
    }

    public Store insertRelTypCharGr(Map<String, Object> rec) throws Exception {
        long id = insertEntity(rec);
        //
        return loadRelTypCharGrRec(id, UtCnv.toString(rec.get("lang")));
    }

    public Store updateRelTypCharGr(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        if (id == 0) {
            throw new XError("Поле id должно иметь не нулевое значение");
        }
        updateEntity(rec);
        //
        return loadRelTypCharGr(id, UtCnv.toString(rec.get("lang")));
    }

    public void deleteRelTypCharGr(long id) throws Exception {
        deleteEntity(Map.of("id", id));
    }

    //RelTypCharGrProp
    public Store loadRelTypCharGrProp(Map<String, Object> params) throws Exception {
        String lang = UtCnv.toString(params.get("lang"));
        //RelTypCharGrProp
        long relTypCharGr = UtCnv.toLong(params.get("relTypCharGr"));
        long relTyp = UtCnv.toLong(params.get("typORrel"));
        Store st = mdb.createStore("RelTypCharGrProp.prop");
        String sql = """
                    select
                        tcp.id as relTypCharGrProp, relTypCharGrProp_measure, pm.name as p_measure, propVal_measure,
                        m.name as pv_measure, storageType, 'p_'||p.id as id,
                        case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                        p.id as prop, p.propgr, p.propType, p.cod, p.name, flatTable
                    from RelTypCharGrProp tcp
                        left join prop p on p.id=tcp.prop
                        left join propVal pv on pv.id=tcp.propval_measure
                        left join Measure m on m.id=pv.measure
                        left join prop pm on pm.id=tcp.relTypCharGrprop_measure
                    where tcp.relTypCharGr=:tcg and tcp.prop is not null
                """;
        if (relTyp > 0)
            sql = """
                        with rtcg as (
                            select distinct id from reltypchargr where reltyp=:reltyp
                        )
                        select distinct
                            'p_'||p.id as id,
                            case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                            p.id as prop, p.propgr, p.propType, p.cod, p.name, flatTable
                        from reltypchargrprop rtcp
                            left join prop p on p.id=rtcp.prop
                        where rtcp.prop is not null
                            and rtcp.reltypchargr in (select id from rtcg)
                    """;


        mdb.loadQuery(st, sql, Map.of("tcg", relTypCharGr, "reltyp", relTyp));

        //mdb.outTable(st);


        // PropGr
        Store stGr = mdb.createStore("RelTypCharGrProp.propGr");
        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr
                    from PropGr where 0=0
                """;
        mdb.loadQuery(stGr, sqlGr);
        StoreIndex indStGr = stGr.getIndex("id");

        //setParents for Meter, Factor... without MeterRate
        Set<Object> idsGr = new HashSet<>();
        for (StoreRecord r1 : st) {
            if (r1.getString("parent").startsWith("p_"))
                continue;
            String curIdgr = "";
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("propGr") == r2.getLong("propGr")) {
                    curIdgr = r2.getString("id");
                    break;
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr);
            idsGr.add(recGr.getString("id").substring(2));
            Object parentGr = recGr.get("parent");
            while (parentGr != "") {
                idsGr.add(UtCnv.toString(parentGr).substring(2));
                recGr = indStGr.get(parentGr);
                parentGr = recGr.get("parent");
            }
        }

        //Parents stGr
        String whe = String.join(",", UtCnv.toString(idsGr))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        stGr = mdb.createStore("RelTypCharGrProp.prop");
        sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr, cod, name
                    from PropGr where id in
                """ + whe;

        mdb.loadQuery(stGr, sqlGr);
        stGr.add(st);
        //mdb.outTable(stGr);

        Store stGrAll = mdb.createStore("RelTypCharGrProp.prop");
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
                """;
        mdb.loadQuery(stGrAll, sqlGrAll);
        StoreIndex indStAll = stGrAll.getIndex("id");
        //mdb.outTable(stGrAll);


        //Analize parents
        Set<Object> ids = stGr.getUniqueValues("id");
        Set<Object> setPropGr = new HashSet<>();
        Set<Object> setProp = new HashSet<>();
        for (StoreRecord r : stGr) {
            if (r.getString("parent").isBlank())
                continue;
            if (!ids.contains(r.getString("parent"))) {
                setPropGr.add(r.getLong("propGr"));
                setProp.add(r.getLong("prop"));
            }
        }

        if (setProp.isEmpty()) {
            Stream<StoreRecord> strm = stGr.getRecords().stream().filter(r -> r.getLong("propType") == FD_PropType_consts.complex);
            List<String> lst = new ArrayList<>();
            List<String> lstTmp1 = new ArrayList<>();
            strm.forEach(s -> lst.add(s.getString("id")));
            //
            stGr.getRecords().forEach(r -> {
                if (lst.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                    lstTmp1.add(r.getString("id"));
                }
            });
            // ToDo
            List<String> lstTmp2 = new ArrayList<>();
            stGr.getRecords().forEach(r -> {
                if (lstTmp1.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                    lstTmp2.add(r.getString("id"));
                }
            });
            List<String> lstTmp3 = new ArrayList<>();
            stGr.getRecords().forEach(r -> {
                if (lstTmp2.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                    lstTmp3.add(r.getString("id"));
                }
            });
            //Last 4 lev
            stGr.getRecords().forEach(r -> {
                if (lstTmp3.contains(r.getString("parent"))) {
                    r.set("isItem", true);
                }
            });
            return stGr;
        }

        String whGr = String.join(",", UtCnv.toString(setPropGr))
                .replace("[", "(").replace("]", ")");
        if (whGr.equals("()")) whGr = "(0)";
        Store stPath = mdb.createStore("DomainPath");
        mdb.loadQuery(stPath, "select id, parent, '' as path from Prop where propGr in " + whGr);
        UtMeterSoft utMeterSoft = new UtMeterSoft(mdb, 0, lang, false);
        utMeterSoft.setPath(stPath);
        //mdb.outTable(stPath);
        StoreIndex indStProp = stPath.getIndex("id");
        StoreIndex indStGrProp = stGr.getIndex("id");

        Store stGrCopy = mdb.createStore("RelTypCharGrProp.prop");
        Set<Object> idsPropGrDop = new HashSet<>();

        for (Object it : setPropGr) {
            String gr = "g_" + UtCnv.toString(it);
            for (StoreRecord r : stGr) {
                if (!setProp.contains(r.getLong("prop"))) continue;
                StoreRecord rec = indStProp.get(r.getLong("prop"));
                List<String> lstPrts = List.of(rec.getString("path").split(","));
                for (String el : lstPrts) {
                    boolean ok = false;
                    if (ids.contains("p_" + el)) {
                        r.set("parent", "p_" + el);
                        ok = true;
                        break;
                    }
                    if (!ok) {
                        StoreRecord recGr = indStGrProp.get(gr);
                        if (recGr != null)
                            r.set("parent", recGr.getString("parent"));
                        else {
                            recGr = indStAll.get(gr);
                            while (true) {
                                if (recGr.getString("id").startsWith("g_"))
                                    break;
                                recGr = indStAll.get(recGr.getString("parent"));
                            }
                            r.set("parent", recGr.getString("parent"));
                            idsPropGrDop.add(recGr.getLong("propGr"));
                        }
                    }
                }
            }
        }

        //Defined groups for PropMeter
        whe = String.join(",", UtCnv.toString(idsPropGrDop))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        mdb.loadQuery(stGrCopy, sqlGr);
        //
        stGr.add(stGrCopy);

        //mdb.outTable(stGrCopy);
        //mdb.outTable(stGr);
        return stGr;
    }

    public Store loadRelTypCharGrPropForUpd(long relTypCharGr) throws Exception {
        //old values
        Store stOld = loadRelTypCharGrProp(Map.of("relTypCharGr", relTypCharGr));
        StoreIndex indStOld = stOld.getIndex("id");

        Store st = mdb.createStore("RelTypCharGrProp.prop.checked");
        String sql = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr,
                    null as prop, null as propType, cod, name, false as checked
                    from PropGr where 0=0
                    union all
                    select 'p_'||p.id as id,
                    case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent, p.propgr,
                    p.id as prop, p.propType, p.cod, p.name, false as checked
                    from Prop p
                    where 0=0
                """;
        mdb.loadQuery(st, sql);
        List<String> lst = new ArrayList<>();
        for (StoreRecord r : st) {
            if (r.getLong("propType") == FD_PropType_consts.complex)
                lst.add(r.getString("id"));
            StoreRecord rec = indStOld.get(r.getString("id"));
            if (rec != null) {
                r.set("checked", true);
                r.set("relTypCharGrProp", rec.getLong("relTypCharGrProp"));
            }
        }
        //
        List<String> lstTmp1 = new ArrayList<>();
        st.getRecords().forEach(r -> {
            if (lst.contains(r.getString("parent"))) {
                r.set("isItem", true);
                lstTmp1.add(r.getString("id"));
            }
        });
        //ToDo
        List<String> lstTmp2 = new ArrayList<>();
        st.getRecords().forEach(r -> {
            if (lstTmp1.contains(r.getString("parent"))) {
                r.set("isItem", true);
                lstTmp2.add(r.getString("id"));
            }
        });
        List<String> lstTmp3 = new ArrayList<>();
        st.getRecords().forEach(r -> {
            if (lstTmp2.contains(r.getString("parent"))) {
                r.set("isItem", true);
                lstTmp3.add(r.getString("id"));
            }
        });
        //Last 4 lev
        st.getRecords().forEach(r -> {
            if (lstTmp3.contains(r.getString("parent"))) {
                r.set("isItem", true);
            }
        });

        return st;
    }

    public String saveRelTypCharGrProps(Map<String, Object> params) throws Exception {
        long relTypCharGr = UtCnv.toLong(params.get("relTypCharGr"));
        List<Map<String, Object>> lstData = (List<Map<String, Object>>) params.get("data");

        //Old Props in RelTypCharGrProp
        Store stOld = mdb.loadQuery("""
                    select c.id, c.prop, p.cod from RelTypCharGrProp c, Prop p
                    where c.prop=p.id and c.relTypCharGr=:g
                """, Map.of("g", relTypCharGr));
        Set<Object> oldPropIds = stOld.getUniqueValues("prop");
        Set<Object> oldIds = stOld.getUniqueValues("id");
        //todo Old Props in CharGrProp with Data
        Set<Object> oldPropsData = new HashSet<>();
        /*
        делаем запрос и получаем oldPropsData
        */
        // пока
        oldPropsData = Set.of(0);

        //allIds = oldIds + newIds
        Set<Object> allIds = new HashSet<>();
        for (Map<String, Object> map : lstData) {
            allIds.add(UtCnv.toLong(map.get("relTypCharGrProp")));
        }
        //Deleting
        Set<String> codsPropForInfo = new HashSet<>();
        for (StoreRecord r : stOld) {
            if (!allIds.contains(r.getLong("id"))) {
                if (oldPropsData.contains(r.getLong("prop"))) {
                    codsPropForInfo.add(r.getString("cod"));
                } else {
                    try {
                        mdb.deleteRec("RelTypCharGrProp", r.getLong("id"));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
        // Saving
        Store st = mdb.createStore("RelTypCharGrProp");
        for (Map<String, Object> map : lstData) {
            if (!oldIds.contains(UtCnv.toLong(map.get("relTypCharGrProp")))) {
                StoreRecord r = st.add(map);
                r.set("id", null);
                r.set("prop", UtCnv.toLong(map.get("prop")));
                r.set("relTypCharGr", relTypCharGr);
                r.set("storageType", FD_StorageType_consts.std);
                mdb.insertRec("RelTypCharGrProp", r, true);
            }
        }

        return String.join(",", codsPropForInfo);
    }

    //RelTypCharGrMultiProp
    public Store loadRelTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        long relTypCharGr = UtCnv.toLong(params.get("relTypCharGr"));
        long relTyp = UtCnv.toLong(params.get("typORrel"));

        Store st = mdb.createStore("RelTypCharGrProp.multiProp");
        String sql = """
                    select tcp.id as relTypCharGrProp, storageType,
                    'p_'||p.id as id, 'g_'||p.multiPropGr as parent, p.id as multiProp, p.multiPropGr, p.cod, p.name
                    from RelTypCharGrProp tcp
                    left join MultiProp p on p.id=tcp.multiProp
                    where tcp.relTypCharGr=:tcg and tcp.multiProp is not null
                """;
        if (relTyp > 0)
            sql = """
                        with rtcg as (
                            select distinct id from reltypchargr where reltyp=:reltyp
                        )
                        select 'p_'||p.id as id, 'g_'||p.multiPropGr as parent,
                        p.id as multiProp, p.multiPropGr, p.cod, p.name
                        from RelTypCharGrProp tcp
                        left join MultiProp p on p.id=tcp.multiProp
                        where tcp.multiProp is not null and tcp.reltypchargr in (select id from rtcg)
                    """;

        mdb.loadQuery(st, sql, Map.of("tcg", relTypCharGr));

        // MultiPropGr
        Store stGr = mdb.createStore("RelTypCharGrProp.multiPropGr");
        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as multiPropGr
                    from MultiPropGr where 0=0
                """;
        mdb.loadQuery(stGr, sqlGr);
        StoreIndex indStGr = stGr.getIndex("id");
        //setParents
        Set<Object> idsGr = new HashSet<>();
        for (StoreRecord r1 : st) {
            String curIdgr = "";
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("multiPropGr") == r2.getLong("multiPropGr")) {
                    curIdgr = r2.getString("id");
                    break;
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr);
            if (recGr != null) {
                idsGr.add(recGr.getString("id").substring(2));
                Object parentGr = recGr.get("parent");
                while (parentGr != "") {
                    idsGr.add(UtCnv.toString(parentGr).substring(2));
                    recGr = indStGr.get(parentGr);
                    parentGr = recGr.get("parent");
                }
            }
        }
        //Parents stGr
        String whe = String.join(",", UtCnv.toString(idsGr))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        stGr = mdb.createStore("RelTypCharGrProp.multiProp");
        sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as multiPropGr, cod, name
                    from MultiPropGr where id in
                """ + whe;

        mdb.loadQuery(stGr, sqlGr);
        stGr.add(st);
        return stGr;
    }

    public Store loadRelTypCharGrMultiPropForUpd(long relTypCharGr) throws Exception {
        //old values
        Store stOld = loadRelTypCharGrMultiProp(Map.of("relTypCharGr", relTypCharGr));
        StoreIndex indStOld = stOld.getIndex("id");
        Store st = mdb.createStore("RelTypCharGrProp.multiProp.checked");
        String sql = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as multiPropGr,
                    null as multiProp, cod, name, false as checked
                    from MultiPropGr where 0=0
                    union all
                    select 'p_'||p.id as id, 'g_'||p.multiPropGr as parent, p.multiPropGr,
                    p.id as multiProp, p.cod, p.name, false as checked
                    from MultiProp p
                    where 0=0
                """;
        mdb.loadQuery(st, sql);
        for (StoreRecord r : st) {
            StoreRecord rec = indStOld.get(r.getString("id"));
            if (rec != null) {
                r.set("checked", true);
                r.set("relTypCharGrProp", rec.getLong("relTypCharGrProp"));
            }
        }

        //
        //mdb.outTable(stOld);
        //mdb.outTable(st);
        //
        return st;
    }

    public String saveRelTypCharGrMultiProp(Map<String, Object> params) throws Exception {
        long relTypCharGr = UtCnv.toLong(params.get("relTypCharGr"));
        List<Map<String, Object>> lstData = (List<Map<String, Object>>) params.get("data");

        //Old multiProps in TypCharGrProp
        Store stOld = mdb.loadQuery("""
                    select c.id, c.multiProp, p.cod from RelTypCharGrProp c, MultiProp p
                    where c.multiProp=p.id and c.relTypCharGr=:g
                """, Map.of("g", relTypCharGr));
        Set<Object> oldPropIds = stOld.getUniqueValues("multiProp");
        Set<Object> oldIds = stOld.getUniqueValues("id");

        //todo Old Props in RelTypCharGrProp with Data
        Set<Object> oldPropsData = new HashSet<>();
        /*
        делаем запрос и получаем oldPropsData
        */
        // пока
        oldPropsData = Set.of(0);

        //allIds = oldIds + newIds
        Set<Object> allIds = new HashSet<>();
        for (Map<String, Object> map : lstData) {
            allIds.add(UtCnv.toLong(map.get("relTypCharGrProp")));
        }
        //Deleting
        Set<String> codsPropForInfo = new HashSet<>();
        for (StoreRecord r : stOld) {
            if (!allIds.contains(r.getLong("id"))) {
                if (oldPropsData.contains(r.getLong("multiProp"))) {
                    codsPropForInfo.add(r.getString("cod"));
                } else {
                    try {
                        mdb.deleteRec("RelTypCharGrProp", r.getLong("id"));
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }

        // Saving
        Store st = mdb.createStore("RelTypCharGrProp");
        for (Map<String, Object> map : lstData) {
            if (!oldIds.contains(UtCnv.toLong(map.get("relTypCharGrProp")))) {
                StoreRecord r = st.add(map);
                r.set("id", null);
                r.set("multiProp", UtCnv.toLong(map.get("multiProp")));
                r.set("relTypCharGr", relTypCharGr);
                r.set("storageType", FD_StorageType_consts.std);
                mdb.insertRec("RelTypCharGrProp", r, true);
            }
        }
        return String.join(",", codsPropForInfo);
    }

    public void updateRelTypCharGrMultiProp(Map<String, Object> rec) throws Exception {
        mdb.updateRec("RelTypCharGrProp", Map.of("id", UtCnv.toLong(rec.get("relTypCharGrProp")),
                "storageType", UtCnv.toLong(rec.get("storageType"))));
        //
    }

    public void updateRelTypCharGrProp(Map<String, Object> rec) throws Exception {

        Long vft = null;
        if (UtCnv.toLong(rec.get("flatTable")) > 0)
            vft = UtCnv.toLong(rec.get("flatTable"));

        Map<String, Long> map = new HashMap<>();
        map.put("id", UtCnv.toLong(rec.get("relTypCharGrProp")));
        map.put("storageType", UtCnv.toLong(rec.get("storageType")));
        map.put("flatTable", vft);

        if (UtCnv.toLong(rec.get("relTypCharGrProp_measure")) > 0)
            map.put("relTypCharGrProp_measure", UtCnv.toLong(rec.get("relTypCharGrProp_measure")));
        if (UtCnv.toLong(rec.get("propVal_measure")) > 0)
            map.put("propVal_measure", UtCnv.toLong(rec.get("propVal_measure")));
        //
        mdb.updateRec("RelTypCharGrProp", map);
        //
        // if ComplexType for childs...
        if (UtCnv.toLong(rec.get("propType")) == FD_PropType_consts.complex) {
            List<Map<String, Object>> lst = UtJson.fromJson(UtCnv.toString(rec.get("children")), List.class);

            for (Map<String, Object> rc : lst) {
                map = new HashMap<>();
                map.put("id", UtCnv.toLong(rc.get("relTypCharGrProp")));
                map.put("storageType", UtCnv.toLong(rc.get("storageType")));
                map.put("flatTable", vft);
                if (UtCnv.toLong(rc.get("relTypCharGrProp_measure")) > 0)
                    map.put("relTypCharGrProp_measure", UtCnv.toLong(rc.get("relTypCharGrProp_measure")));
                if (UtCnv.toLong(rc.get("propVal_measure")) > 0)
                    map.put("propVal_measure", UtCnv.toLong(rc.get("propVal_measure")));
                //
                mdb.updateRec("RelTypCharGrProp", map);
            }
        }

    }

    public Store loadPropMeasure(long relTypCharGr) throws Exception {

        Store st = mdb.createStore("RelTypCharGrProp.prop");
        String sql = """
                    select tcp.id as relTypCharGrProp,
                    'p_'||p.id as id, case when p.parent is null then 'g_'||p.propGr else 'p_'||p.parent end as parent,
                    p.id as prop, p.propgr, p.propType, p.cod, p.name
                    from RelTypCharGrProp tcp
                    left join prop p on p.id=tcp.prop
                    where tcp.relTypCharGr=:tcg and tcp.prop is not null and p.proptype=:pt
                """;
        mdb.loadQuery(st, sql, Map.of("tcg", relTypCharGr, "pt", FD_PropType_consts.measure));

        //mdb.outTable(st);


        // PropGr
        Store stGr = mdb.createStore("RelTypCharGrProp.propGr");
        String sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr, 0 as prop
                    from PropGr where 0=0
                """;
        mdb.loadQuery(stGr, sqlGr);
        StoreIndex indStGr = stGr.getIndex("id");

        //setParents for Meter, Factor... without MeterRate
        Set<Object> idsGr = new HashSet<>();
        for (StoreRecord r1 : st) {
            if (r1.getString("parent").startsWith("p_"))
                continue;
            String curIdgr = "";
            for (StoreRecord r2 : stGr) {
                if (r1.getLong("propGr") == r2.getLong("propGr")) {
                    curIdgr = r2.getString("id");
                    break;
                }
            }
            StoreRecord recGr = indStGr.get(curIdgr);
            idsGr.add(recGr.getString("id").substring(2));
            Object parentGr = recGr.get("parent");
            while (parentGr != "") {
                idsGr.add(UtCnv.toString(parentGr).substring(2));
                recGr = indStGr.get(parentGr);
                parentGr = recGr.get("parent");
            }
        }

        //Parents stGr
        String whe = String.join(",", UtCnv.toString(idsGr))
                .replace("[", "(").replace("]", ")");
        if (whe.equals("()")) whe = "(0)";
        stGr = mdb.createStore("RelTypCharGrProp.prop");
        sqlGr = """
                    select 'g_'||id as id, 'g_'||parent as parent, id as propGr, 0 as prop, cod, name
                    from PropGr where id in
                """ + whe;

        mdb.loadQuery(stGr, sqlGr);
        stGr.add(st);
        //mdb.outTable(stGr);
        return stGr;
    }

    public Store loadMeasure(long prop) throws Exception {
        return mdb.loadQuery("""
                    select m.id, m.name, m.parent, v.id as propval from Measure m, PropVal v
                    where prop=:prop and m.id=v.measure
                """, Map.of("prop", prop));
    }

}
