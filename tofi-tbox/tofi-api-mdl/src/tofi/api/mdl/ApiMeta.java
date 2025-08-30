package tofi.api.mdl;

import jandcode.commons.datetime.XDate;
import jandcode.core.store.Store;
import tofi.api.mdl.utils.dbfilestorage.DbFileStorageService;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ApiMeta {

    /**
     * Загрузка словарей
     *
     * @param dictName Имя словаря
     * @return [ id: text ]
     */
    Map<Long, String> loadDict(String dictName);

    /**
     *
     * @param dictName  Имя словаря
     * @return [ {id:1, text: "aaa" } ]
     */
    Store loadDictAsStore(String dictName);

    /**
     * Возвращает дерево-классов с учетом наследования
     *  1. loadClsTree([typ:0]) - Все типы со своими классами
     *  2. loadClsTree([typ:5000]) - Тип (id=5000) и все его классы
     *  3. loadClsTree([typ:5000, typNodeVisible:false]) - Все классы типа с id=5000, без родителя
     *
     *
     * @param params Map
     * @return Store Tree
     */
    Store loadClsTree(Map<String, Object> params);

    /**
     *
     *
     * @param entity: Entity
     * @param cod: cod of Entity
     * @param prefixcod: prefix of cod Entity
     * @return [codEntity: idEntity]
     */
    Map<String, Long> getIdFromCodOfEntity(String entity, String cod, String prefixcod);

    /**
     *
     * @param cls id Cls
     * @return [{cod: id}]
     */
    Map<String, Long> getIdFromCodOfProp(long cls);

    /**
     *
     * @param codTyp: cod of Typ
     * @return  set of ids Cls
     */
    Set<Object> setIdsOfCls(String codTyp);

    /**
     *
     * @param codRelTyp: cod of RelTyp
     * @return  set of ids RelCls
     */
    Set<Object> setIdsOfRelCls(String codRelTyp);

    /**
     *todo
     * @param entity: {factorval, cls, relcls, measure}
     * @param keyIsPropVal: {true, false}
     * @return if keyIsPropVal is true [idPropVal: idEntity], else [idEntity: idPropVal]
     */
    Map<Long, Long> mapEntityIdFromPV(String entity, boolean keyIsPropVal);

    Map<Long, String> mapPropValArrFromCls(String clsORrelcls);

    /**
     *
     * @return Map[id: name]
     */
    Map<Long, String> mapFvNameFromId();

    /**
     *
     * @param codProp: cod of Prop
     * @return store FV [{id, name,.. propVal}]
     */
    Store storePropValForSelectFV(String codProp);


    /**
     *
     * @return Store [{fv, pv}]
     */
    Store storeFVfromPropVal();

    /**
     *
     * @param entity {factorVal, cls, relCls, measure}
     * @param idEntity {idFactorVal, idCls, idRelCls, idMeasure}
     * @param codProp cod of Prop
     * @return id of PropVal
     */
    long idPV(String entity, long idEntity, String codProp);

    Store getPvFromCls(Set<Object> idsCls, String codProp);

    /**
     *
     * @param cod cod of Prop
     * @return info of Prop
     */
    Store getPropInfo(String cod);

    /**
     *
     * @param tableName name of table
     * @param fkField name of foreign field
     * @param fkValue value of foreign field
     * @param ordField sorted field
     * @return store
     */
    Store getStoreFromFK(String tableName, String fkField, long fkValue, String ordField);


    Store getClsFromTypCharGr(long typId, boolean isMultiProp);

    long getFKFromTable(String tableName, long idTable, String fkField);

    /**
     *
     * @param prop: id of Prop
     * @return default factorVal
     */
    long getDefaultStatus(long prop);

    /**
     *
     * @param entity: Entity, EntityVer
     * @param id: id of Entity
     * @return StoreRecord {id: 1, name: "a", ...}
     */
    Store recEntity(String entity, long id);

    /**
     *
     * @param codTyp cod of Typ
     * @return store
     */
    Store loadCls(String codTyp);

    /**
     *
     * @param codFactor Factor
     * @return Store FactorVals
     */

    Store loadFactorVals(String codFactor);

    Store loadFactorValsWithPV(String codFactor);

    /**
     *
     * @param ft_cod cod Flat Table
     * @return info of Flat Table
     */
    Map<String, Store> infoFlatTable(String ft_cod);

    /**
     *
     * @param codTyp: cod of Typ
     * @return Store Cls
     */
    Store loadClsForSelect(String codTyp);

    /**
     *
     * @return info of Measure (name, kFromBase)
     */
    Map<String, Object> measureInfo();

    Map<Long, String> mapFVforSelect(String codFactor);

    /**
     *
     * @param Entity table Entity
     * @param cods cods of Entity
     * @return ids of Entity
     */
    Map<String, Long> getIdsFromCodOfEntity(String Entity, String cods);

    /**
     *
     * @param sql text sql
     * @return store
     */
    Store loadSql(String sql, String domain);

    /**
     *
     * @param sql text sql
     * @param domain Domain
     * @param params Params
     * @return store
     */
    Store loadSqlWithParams(String sql, String domain, Map<String, Object> params);

    /**
     *
     * @param dt data
     * @param periodType periodType
     * @return info of period
     */
    Map<String, Object> getPeriodInfo(XDate dt, long periodType);

    /**
     *
     * @return DbFileStorageService
     */
    DbFileStorageService getDbFileStorageService();

    /**
     *
     * @param propId id Prop
     * @return коеффициент kFromBase
     */
    double getKfromBase(long propId);

    String ListClsParents(long typ, long cls);

    long createFactorVal(long factor, String nameFV);

    long createCls(long typ, String nameCls);

    void createClsFactorVal(long cls, long fv);

    void createGroupRelCls(long reltyp, long cls1, long typ1, long cls2, long typ2, long db);

    /**
     * @param sql text sql
     */
    void execSql(String sql);

}
