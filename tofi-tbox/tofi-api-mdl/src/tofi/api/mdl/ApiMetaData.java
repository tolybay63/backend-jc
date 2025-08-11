package tofi.api.mdl;

import jandcode.commons.variant.IVariantMap;
import jandcode.core.store.Store;

import java.util.Map;

public interface ApiMetaData {

    /**
     *
     * @return Store of DataBase
     */
    Store loadDataBase();

    /**
     *
     * @param params [dataType: {std|multi|flat}, dataBase: {userdata|kpi|...} ]
     * @return owners first level
     */
    Store loadOwnersParent(Map<String, Object> params);

    /**
     *
     * @param tr typ or relCls
     * @param isFlat is flat Data
     * @return Store
     */
    Store loadTypCharGrProp(long tr, boolean isObj, boolean isFlat);


    /**
     *
     * @param tr typ or relCls
     * @param isObj Obj or RelObj
     * @return Store
     */
    Store loadCharGrMultiProp(long tr, boolean isObj);

    /**
     *
     * @param sql text of Sql
     * @return Store
     */
    Store loadSql(String sql, String domain);

    /**
     *
     * @param prop prop
     * @param entityType entityType
     * @return  store
     */
    Store loadPropValEntityForSelect(long prop, long entityType);    /**

     *
     * @param entity field of entity {factorVal, cls, relcls, measure}
     * @param prop id Prop
     * @return id PV
     */
    Map<Long, Long> mapIdEntityFromPV(String entity, long prop);

}
