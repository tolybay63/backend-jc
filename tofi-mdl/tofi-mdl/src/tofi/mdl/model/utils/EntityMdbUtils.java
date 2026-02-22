package tofi.mdl.model.utils;

import jandcode.commons.UtCnv;
import jandcode.commons.UtLang;
import jandcode.commons.UtString;
import jandcode.commons.error.XError;
import jandcode.core.dbm.domain.Domain;
import jandcode.core.dbm.domain.DomainService;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.mdl.consts.FD_PropType_consts;

import java.util.Map;
import java.util.Objects;

public class EntityMdbUtils {
    Mdb mdb;
    String tableName;

    public EntityMdbUtils(Mdb mdb, String tableName) {
        this.mdb = mdb;
        this.tableName = tableName;
    }

    public void deleteEntity(Map<String, Object> map) throws Exception {
        StoreRecord rec = mdb.createStoreRecord(tableName, map);

        long id = UtCnv.toLong(rec.get("id"));
        String sign = tableName;
        if (tableName.equalsIgnoreCase("Factor")) {
            if (UtCnv.toLong(rec.get("parent")) > 0)
                sign = "FactorVal";
        }

        long ent = EntityConst.getNumConst(sign);

        mdb.execQuery("""
                    delete from SysCod
                    where linkType=:linkType and linkId=:linkId
                """, Map.of("linkType", ent, "linkId", id));

        if (EntityConst.getEntityInfo(ent).getHasVer()) { //todo Проверить
            mdb.execQuery("""
                delete from TableLang
                where nameTable=:tb
                    and idTable in (select id from
            """ + tableName + "Ver where lastVer=1 and ownerVer=:id);"+
            """
                delete from
            """ + tableName + "Ver " + """
            where ownerVer=:id;
            """, Map.of("tb", tableName + "Ver", "id", id));
        } else if (EntityConst.getEntityInfo(ent).getHasTranslate()) {
            mdb.execQuery("delete from TableLang where nameTable=:tb and idTable=:id", Map.of("tb", tableName, "id", id));
        }

        //
        mdb.deleteRec(tableName, id);
    }

    public void deleteEntityVer(Map<String, Object> rec) throws Exception {
        long id = UtCnv.toLong(rec.get("id"));
        long ownerVer = UtCnv.toLong(rec.get("ownerVer"));

        Store vers = mdb.createStore(tableName + "Ver");
        mdb.loadQuery(vers, "select * from " + tableName + "Ver where ownerver=:ow order by dend desc", Map.of("ow", ownerVer));
        if (vers.size() == 1) {
            throw new XError(UtLang.t("Нельзя удалить единственную версию"));
        }
        //
        int k = 0;
        for (int i = 0; i < vers.size(); i++) {
            if (vers.get(i).getLong("id") == id) {
                k = i;
                break;
            }
        }
        mdb.deleteRec(tableName + "Ver", id);
        if (k == 0) {
            mdb.execQuery("update " + tableName + "Ver set lastVer=1 where id=" + vers.get(1).getLong("id"));
        }
        //
        mdb.execQuery("delete from TableLang where nameTable=:tb and idTable=:id", Map.of("tb", tableName+"Ver", "id", id));
    }

    public void updateEntity(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore(tableName);
        if (tableName.equalsIgnoreCase("DimMultiPropItem")) {
            for (String key : rec.keySet()) {
                if (key.startsWith("col_")) {
                    st.addField(key, "string", 150);
                }
            }
        }
        rec.putIfAbsent("accessLevel", 1L);
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        //
        StoreRecord r = st.add(rec);

        long id = r.getLong("id");
        StoreRecord oldRec = mdb.loadQueryRecord("select * from " + tableName + " where id=:id", Map.of("id", id));
        // Checking cod
        String cod = r.getString("cod");
        String oldCod = oldRec.getString("cod");
        //
        if (!UtString.empty(cod) && !cod.equalsIgnoreCase(oldCod)) {
            checkCod(cod);
        }
        //
        long ent = EntityConst.getNumConst(tableName);
        if (tableName.equalsIgnoreCase("Factor")) {
            if (r.getLong("parent") > 0)
                ent = EntityConst.getNumConst("FactorVal");
        }
        // генерим код, если не указан
        if (cod.isEmpty()) {
            cod = EntityConst.generateCod(ent, id);
            r.set("cod", cod);
        }
        // изменяем код
        if (!cod.equalsIgnoreCase(oldCod)) {
            mdb.execQuery("""
                        update SysCod set cod=:cod
                        where linkType=:linkType and linkId=:linkId
                    """, Map.of("cod", cod, "linkType", ent, "linkId", id));
        }

        DomainService domainSvc = mdb.getModel().bean(DomainService.class);
        Domain dm = domainSvc.getDomain(tableName);
        if (dm.findField("parent") != null) {
            if (r.getLong("parent") == 0)
                r.set("parent", null);
        }
        //
        if (dm.findField("ord") != null) {
            r.set("ord", id);
        }
        //

        mdb.updateRec(tableName, r);
        /////
        if (EntityConst.getEntityInfo(ent).getHasVer()) {
            long verId = mdb.loadQuery("select v.id from " + tableName + " t," + tableName + "Ver v " +
                            "where t.id=v.ownerVer and v.lastVer=1 and t.id=:id",
                    Map.of("id", id)).get(0).getLong("id");

            st = mdb.createStore(tableName + "Ver");
            StoreRecord rV = st.add(rec);
            rV.set("id", verId);
            rV.set("ownerVer", id);
            rV.set("lastVer", 1);
            if (tableName.equalsIgnoreCase("obj")) {
                long parent = UtCnv.toLong(rec.get("parent"));
                if (parent > 0)
                    rV.set("objParent", parent);
            }
            mdb.updateRec(tableName + "Ver", rV);
            //Translate
            //idTable == idVer;
            String lang = UtCnv.toString(rec.get("lang"));
            st = mdb.createStore("TableLang");
            mdb.loadQuery(st, """
                        select * from TableLang where nameTable=:tableNameVer and lang=:lang and idTable=:idTable
                    """, Map.of("tableNameVer", tableName+ "Ver", "lang", lang, "idTable", verId));

            if (st.size() > 1)
                throw new XError("Не понятно! Проверить...");
            else if (st.size() == 1) {
                StoreRecord rLang = st.add(rec);
                rLang.set("id", st.get(0).getLong("id"));
                rLang.set("nameTable", tableName+"Ver");
                rLang.set("idTable", verId);
                mdb.updateRec("TableLang", rLang);
            } else if (st.size() == 0) {
                StoreRecord rLang = st.add(rec);
                long idLang = mdb.getNextId("TableLang");
                rLang.set("id", idLang);
                rLang.set("nameTable", tableName+"Ver");
                rLang.set("idTable", verId);
                mdb.insertRec("TableLang", rLang, false);
            }
        } else if (EntityConst.getEntityInfo(ent).getHasTranslate()) {
/*
    tableName - curTable   rec.get("id") - idTable  rec.get("lang") - lang
*/
            //idTable == id;
            String lang = UtCnv.toString(rec.get("lang"));
            st = mdb.createStore("TableLang");
            mdb.loadQuery(st, """
                        select * from TableLang where nameTable=:tableName and lang=:lang and idTable=:idTable
                    """, Map.of("tableName", tableName, "lang", lang, "idTable", id));

            if (st.size() > 1) {
                throw new XError("Не понятно! Надо проверить...");
            } else if (st.size() == 1) {
                StoreRecord rLang = st.add(rec);
                rLang.set("id", st.get(0).getLong("id"));
                rLang.set("nameTable", tableName);
                rLang.set("idTable", id);
                mdb.updateRec("TableLang", rLang);
                //
                st.clear();
                mdb.loadQuery(st, """
                        select * from TableLang where nameTable=:tableName and lang<>:lang and idTable=:idTable
                    """, Map.of("tableName", tableName, "lang", lang, "idTable", id));
                if (!rLang.getString("cmt").isEmpty()) {
                    Translator tr = new Translator(mdb);
                    for (StoreRecord record : st) {
                        if (record.getString("cmt").isEmpty()) {
                            String s = tr.translateText(rLang.getString("cmt"), lang, record.getString("lang"));
                            record.set("cmt", s);
                            mdb.updateRec("TableLang", record);
                        }
                    }
                }
            } else if (st.size() == 0) {
                StoreRecord rLang = mdb.createStoreRecord("TableLang", rec);
                long idLang = mdb.getNextId("TableLang");
                rLang.set("id", idLang);
                rLang.set("nameTable", tableName);
                rLang.set("idTable", id);
                mdb.insertRec("TableLang", rLang, false);
            }
        }

    }

    public void updateEntityVer(Map<String, Object> rec) throws Exception {
        long idVer = UtCnv.toLong(rec.get("id"));
        long ownerVer = UtCnv.toLong(rec.get("ownerVer"));
        //
        Store st = mdb.createStore(tableName + "Ver");
        StoreRecord r = st.add(rec);
        if (Objects.equals(r.getString("dbeg"), "0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (Objects.equals(r.getString("dend"), "0000-01-01"))
            r.set("dend", "3333-12-31");
        //dbeg <= dend
        if (r.getDate("dbeg").toJavaLocalDate().isAfter(r.getDate("dend").toJavaLocalDate())) {
            throw new XError("Дата начала интервала жизни версии не может быть больше даты конца");
        }
        //
        Store vers = mdb.createStore(tableName + "Ver");
        mdb.loadQuery(vers, "select * from " + tableName + "Ver where ownerVer=:ow order by dend desc",
                Map.of("ow", ownerVer));
        int k = 0, lastVer = 0;
        for (int i = 0; i < vers.size(); i++) {
            if (vers.get(i).getLong("id") == idVer) {
                k = i;
                lastVer = vers.get(i).getInt("lastVer");
                break;
            }
        }
        if (k == 0) { //first rec
            if (vers.size() > 1) {
                if (!r.getDate("dbeg").toJavaLocalDate().isAfter(vers.get(k + 1).getDate("dend").toJavaLocalDate())) {
                    throw new XError("Дата начала интервала жизни версии должна быть больше даты конца интервала жизни предыдущей версии");
                }
            }
        } else if (k == vers.size() - 1) { //last rec
            if (!r.getDate("dend").toJavaLocalDate().isBefore(vers.get(k - 1).getDate("dbeg").toJavaLocalDate())) {
                throw new XError("Дата конца интервала жизни версии должна быть меньше даты начало интервала жизни следующей версии");
            }

        } else {
            if (!r.getDate("dbeg").toJavaLocalDate().isAfter(vers.get(k + 1).getDate("dend").toJavaLocalDate())) {
                throw new XError("Дата начала интервала жизни версии должна быть больше даты конца интервала жизни предыдущей версии");
            }
            if (!r.getDate("dend").toJavaLocalDate().isBefore(vers.get(k - 1).getDate("dbeg").toJavaLocalDate())) {
                throw new XError("Дата конца интервала жизни версии должна быть меньше даты начало интервала жизни следующей версии");
            }
        }
        r.set("id", idVer);
        r.set("lastVer", lastVer);
        mdb.updateRec(tableName + "Ver", r);
        //
        //Translate
        //idTable == idVer;
        String lang = UtCnv.toString(rec.get("lang"));
        st = mdb.createStore("TableLang");
        mdb.loadQuery(st, """
            select * from TableLang where nameTable=:tableNameVer and lang=:lang and idTable=:idTable
        """, Map.of("tableNameVer", tableName+"Ver", "lang", lang, "idTable", idVer));

        if (st.size() > 1)
            throw new XError("Не понятно! Проверить...");
        else if (st.size() == 1) {
            StoreRecord rLang = st.add(rec);
            rLang.set("id", st.get(0).getLong("id"));
            rLang.set("nameTable", tableName+"Ver");
            rLang.set("idTable", idVer);
            mdb.updateRec("TableLang", rLang);
        } else if (st.size() == 0) {
            StoreRecord rLang = st.add(rec);
            long idLang = mdb.getNextId("TableLang");
            rLang.set("id", idLang);
            rLang.set("nameTable", tableName+"Ver");
            rLang.set("idTable", idVer);
            mdb.insertRec("TableLang", rLang, false);
        }

    }

    public long insertEntity(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore(tableName);
        rec.putIfAbsent("accessLevel", 1L);
        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        StoreRecord r = st.add(rec);
        String cod = r.getString("cod");
        checkCod(cod);
        long id = mdb.getNextId(tableName);
        r.set("id", id);
        //
        DomainService domainSvc = mdb.getModel().bean(DomainService.class);
        Domain dm = domainSvc.getDomain(tableName);
        if (dm.findField("ord") != null) {
            r.set("ord", id);
        }
        if (tableName.equalsIgnoreCase("Prop")) {
            if (r.getLong("propType") == FD_PropType_consts.meter ||
                    r.getLong("propType") == FD_PropType_consts.rate)
                r.set("isUniq", true);
        }
        //
        long ent = EntityConst.getNumConst(tableName);
        if (tableName.equalsIgnoreCase("Factor")) {
            if (r.getLong("parent") > 0)
                ent = EntityConst.getNumConst("FactorVal");
        }
        if (cod.isEmpty()) {
            cod = EntityConst.generateCod(ent, id);
            r.set("cod", cod);
        }
        //
        mdb.insertRec(tableName, r, false);
        // добавляем код
        mdb.insertRec("SysCod", Map.of("cod", cod, "linkType", ent, "linkId", id));

        if (EntityConst.getEntityInfo(ent).getHasVer()) {
            st = mdb.createStore(tableName + "Ver");
            StoreRecord rV = st.add(rec);

            long idVer = mdb.getNextId(tableName + "Ver");
            rV.set("id", idVer);
            rV.set("ownerVer", id);
            rV.set("lastVer", 1);

            if (tableName.equalsIgnoreCase("obj")) {
                long parent = UtCnv.toLong(rec.get("parent"));
                if (parent > 0)
                    rV.set("objParent", parent);
            }
            mdb.insertRec(tableName + "Ver", rV, false);
            //
            if (EntityConst.getEntityInfo(ent).getHasTranslate()) {
                st = mdb.createStore("TableLang");
                StoreRecord rLang = st.add(rec);
                long idLang = mdb.getNextId("TableLang");
                rLang.set("id", idLang);
                rLang.set("nameTable", tableName+"Ver");
                rLang.set("idTable", idVer);
                mdb.insertRec("TableLang", rLang, false);
            }
        } else if (EntityConst.getEntityInfo(ent).getHasTranslate()) {
            st = mdb.createStore("TableLang");
            StoreRecord rLang = st.add(rec);
            long idLang = mdb.getNextId("TableLang");
            rLang.set("id", idLang);
            rLang.set("nameTable", tableName);
            rLang.set("idTable", id);
            mdb.insertRec("TableLang", rLang, false);
        }

        return id;
    }

    public long insertEntityVer(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore(tableName + "Ver");
        StoreRecord r = st.add(rec);
        long ownerVer = r.getLong("ownerVer");
        //dbeg <= dend
        if (Objects.equals(r.getString("dbeg"), "0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (Objects.equals(r.getString("dend"), "0000-01-01"))
            r.set("dend", "3333-12-31");
        if (r.getDate("dbeg").toJavaLocalDate().isAfter(r.getDate("dend").toJavaLocalDate())) {
            throw new XError("Дата начала интервала жизни версии не может быть больше даты конца");
        }
        //last(dbeg) <= prev(dend)
        StoreRecord prevRec = mdb.loadQuery("select * from " + tableName + "Ver where ownerVer=:ow order by dend desc",
                Map.of("ow", ownerVer)).get(0);
        if (!r.getDate("dbeg").toJavaLocalDate().isAfter(prevRec.getDate("dend").toJavaLocalDate())) {
            throw new XError("Дата начала интервала жизни версии должна быть больше даты начала интервала жизни предыдущей версии");
        }
        //
        mdb.execQuery("update " + tableName + "Ver set lastVer=0 where ownerVer=:ow",
                Map.of("ow", ownerVer));
        //

        long id = mdb.getNextId(tableName + "Ver");
        r.set("id", id);
        r.set("lastVer", 1);
        mdb.insertRec(tableName + "Ver", r, false);
        // for Translate
        Store stLang = mdb.createStore("TableLang");
        StoreRecord rLang = stLang.add(rec);
        long idLang = mdb.getNextId("TableLang");
        rLang.set("id", idLang);
        rLang.set("nameTable", tableName+"Ver");
        rLang.set("idTable", id);
        mdb.insertRec("TableLang", rLang, false);
        //
        return id;
    }

    public void checkCod(String cod) throws Exception {
        if (cod.startsWith(EntityConst.genCodPref)) {
            throw new XError(UtLang.t("Код не может начинаться с символа «_»"), "cod");
        }
        if (!EntityConst.isCodValid(cod)) {
            throw new XError(UtLang.t("Допустимыми символами для кода являются: " +
                    "цифры, буквы на английском, «_», « - », « / », « . »"), "cod");
        }
        checkCodUnique(cod);
    }

    /**
     * Проверка уникальности кода сущности
     */
    protected void checkCodUnique(String cod) throws Exception {
        Store ds = mdb.loadQuery("""                        
                    select linktype, linkid from syscod
                    where UPPER(cod)=UPPER(:cod)
                """, Map.of("cod", cod));

        if (ds.size() > 0) {
            long linkType = ds.get(0).getLong("linktype");
            long linkId = ds.get(0).getLong("linkid");
            EntityConst.EntityInfo entityInfo = EntityConst.getEntityInfo(linkType);
            String tableName = entityInfo.getTableName();
            String sql = "select name from " + tableName + " where id=:id";
            Store st = mdb.loadQuery(sql, Map.of("id", linkId));
            String inst = st.get(0).getString("name");
            throw new XError(UtLang.t("Введенный код является кодом экземпляра {1} сущности {0}", UtLang.t(entityInfo.getText()), inst), "cod");
        }

    }

}
