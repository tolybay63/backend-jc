package tofi.api.mdl;

import jandcode.core.store.Store;

import java.util.Map;
import java.util.Set;

public interface ApiMetaFish {

    /**
    /**
     *
     * @param codRelTyp cod of RelTyp or Typ
     * @param isRel  if true RelTyp, false Typ
     * @return Info of Measure codProp[mesurename, kfc, minVal, maxVal]
     */
    Map<String, String> getMeasureInfo(String codRelTyp, boolean isRel);

    long idRelCls(long cls1, long cls2);

    /**
     *
     * @param codTyp cod Typ
     * @return  all child Clsasses
     */
    Set<Object> idsChildClses(String codTyp);

    /**
     *
     * @param codTyp cod Typ
     * @return  Store Obj
     */
    Store loadChildClsForSelect(String codTyp);

    /**
     *
     * @param codFactor cod of Factor
     * @return  store FV from PropVal
     */
    Store loadFvForSelect(String codFactor);

    /**
     *
     * @param relcls id of RelCls
     * @return RelClsMember
     */
    Store loadRelClsMember(long relcls);


}
