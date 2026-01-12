package tofi.api.mdl.utils;

import jandcode.commons.UtCnv;
import jandcode.commons.UtLang;
import jandcode.commons.UtString;
import jandcode.commons.datetime.XDateTime;
import jandcode.commons.datetime.XDateTimeFormatter;
import jandcode.commons.error.XError;
import jandcode.core.dbm.domain.Domain;
import jandcode.core.dbm.domain.DomainService;
import jandcode.core.dbm.mdb.Mdb;
import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;
import tofi.api.mdl.model.consts.*;


import java.util.Date;
import java.util.Map;

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

        if (EntityConst.getEntityInfo(ent).getHasVer()) {
            mdb.execQuery("delete from " + tableName + "Ver where ownerVer=:id", Map.of("id", id));
        }
        //
        mdb.deleteRec(tableName, id);
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
        if (dm.findField("ord") != null) {
            r.set("ord", id);
        }
        if (dm.findField("timeStamp") != null)
            r.set("timeStamp", XDateTime.create(new Date()).toString(XDateTimeFormatter.ISO_DATE_TIME));
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
        }
    }

    public long insertEntity(Map<String, Object> rec) throws Exception {
        DomainService domainSvc = mdb.getModel().bean(DomainService.class);
        Domain dm = domainSvc.getDomain(tableName);

        Store st = mdb.createStore(tableName);
        if (dm.findField("accessLevel") != null)
            rec.putIfAbsent("accessLevel", 1L);

        rec.putIfAbsent("dbeg", "1800-01-01");
        rec.putIfAbsent("dend", "3333-12-31");
        StoreRecord r = st.add(rec);
        String cod = r.getString("cod");
        checkCod(cod);
        long id = mdb.getNextId(tableName);
        r.set("id", id);
        //
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
        }

        return id;
    }

    public long insertEntityVer(Map<String, Object> rec) throws Exception {
        Store st = mdb.createStore(tableName + "Ver");
        StoreRecord r = st.add(rec);
        long ownerVer = r.getLong("ownerVer");
        //dbeg <= dend
        if (r.getString("dbeg").equals("0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (r.getString("dend").equals("0000-01-01"))
            r.set("dend", "3333-12-31");
        if (r.getDate("dbeg").toJavaLocalDate().isAfter(r.getDate("dend").toJavaLocalDate())) {
            throw new XError(UtLang.t("Дата начала интервала жизни версии не может быть больше даты конца"));
        }
        //last(dbeg) <= prev(dend)
        StoreRecord prevRec = mdb.loadQuery("select * from " + tableName + "Ver where ownerVer=:ow order by dend desc",
                Map.of("ow", ownerVer)).get(0);
        if (!r.getDate("dbeg").toJavaLocalDate().isAfter(prevRec.getDate("dend").toJavaLocalDate())) {
            throw new XError(UtLang.t("Дата начала интервала жизни версии должна быть больше даты начала интервала жизни предыдущей версии"));
        }
        //
        mdb.execQuery("update " + tableName + "Ver set lastVer=0 where ownerVer=:ow",
                Map.of("ow", ownerVer));
        //

        long id = mdb.getNextId(tableName + "Ver");
        r.set("id", id);
        r.set("lastVer", 1);
        //
        mdb.insertRec(tableName + "Ver", r, false);
        //
        return id;
    }

    public void updateEntityVer(Map<String, Object> rec) throws Exception {
        long idVer = UtCnv.toLong(rec.get("id"));
        long ownerVer = UtCnv.toLong(rec.get("ownerVer"));
        //
        Store st = mdb.createStore(tableName + "Ver");
        StoreRecord r = st.add(rec);
        if (r.getString("dbeg").equals("0000-01-01"))
            r.set("dbeg", "1800-01-01");
        if (r.getString("dend").equals("0000-01-01"))
            r.set("dend", "3333-12-31");
        //dbeg <= dend
        if (r.getDate("dbeg").toJavaLocalDate().isAfter(r.getDate("dend").toJavaLocalDate())) {
            throw new XError(UtLang.t("Дата начала интервала жизни версии не может быть больше даты конца"));
        }
        //
        Store vers = mdb.createStore(tableName + "Ver");
        mdb.loadQuery(vers, "select * from " + tableName + "Ver where ownerver=:ow order by dend desc",
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
                    throw new XError(UtLang.t("Дата начала интервала жизни версии должна быть больше даты конца интервала жизни предыдущей версии"));
                }
            }
        } else if (k == vers.size() - 1) { //last rec
            if (!r.getDate("dend").toJavaLocalDate().isBefore(vers.get(k - 1).getDate("dbeg").toJavaLocalDate())) {
                throw new XError(UtLang.t("Дата конца интервала жизни версии должна быть меньше даты начало интервала жизни следующей версии"));
            }

        } else {
            if (!r.getDate("dbeg").toJavaLocalDate().isAfter(vers.get(k + 1).getDate("dend").toJavaLocalDate())) {
                throw new XError(UtLang.t("Дата начала интервала жизни версии должна быть больше даты конца интервала жизни предыдущей версии"));
            }
            if (!r.getDate("dend").toJavaLocalDate().isBefore(vers.get(k - 1).getDate("dbeg").toJavaLocalDate())) {
                throw new XError(UtLang.t("Дата конца интервала жизни версии должна быть меньше даты начало интервала жизни следующей версии"));
            }
        }
        r.set("id", idVer);
        r.set("lastVer", lastVer);
        mdb.updateRec(tableName + "Ver", r);
        //
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
