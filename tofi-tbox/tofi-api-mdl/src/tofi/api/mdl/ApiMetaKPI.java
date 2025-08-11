package tofi.api.mdl;

import jandcode.core.store.Store;
import jandcode.core.store.StoreRecord;

import java.util.Map;
import java.util.Set;

public interface ApiMetaKPI {

    /**
     *
     * @param factor id Factor
     * @return Store FactorVals
     */
    Store loadFactorVals(long factor);

    /**
     *
     * @param rec FactorVal
     * @return Store
     */
    Store insertFactorVal(Map<String, Object> rec);

    /**
     *
     * @param rec FactorVal
     * @return Store
     */
    Store updateFactorVal(Map<String, Object> rec);

    /**
     *
     * @param rec - FactorVal
     */
    void deleteFactorVal(Map<String, Object> rec);

    /**
     *
     * @param rec Factor
     */
    void changeOrdFV(Map<String, Object> rec);

    /**
     *
     * @param codProp cod of Prop
     * @return Record Info of Prop and Measure
     */
    StoreRecord propAndMeasureInfo(String codProp);

    /**
     *
     * @param codCls cod of Cls
     * @param codProp cod of Prop
     * @return id of Cls & name of Measure
     */
    Map<String, Object> idClsAndMeasureName(String codCls, String codProp);

    /**
     *
     * @param codRelCls cod of RelCls
     * @return id of RelCls & Info of Measure codProp[mesurename, kfc, minVal, maxVal]
     */
    Map<String, Object> idRelClsAndMeasureInfo(String codRelCls);

    Store loadClsForSelect(String codTyp, String codCls);

    Set<Object> setClsWithoutCurCodCls(String codCls);

    /**
     *
     * @param relcls id of RelCls
     * @return RelClsMember
     */
    Store loadRelClsMember(long relcls);

    Map<String, Long> getIdsPropFromRTCG(String codRelCls);
}
