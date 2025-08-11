/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Возможные значения элемента измерения многомерного свойства типа значение фактора
* <p>
    * table: DimMultiPropItemFactor
    */
    public class DimMultiPropItemFactor_dto {

    private Long id;

    private Long dimMultiPropItem;

    private Long factor;

    @FieldProps(dict = "FD_VisualFormat")
    private Long visualFormat;

    /**
    * id
    */
    public Long getId() {
    return this.id;
    }

    public void setId(Long v) {
    this.id = v;
    }

    /**
    * Элементы измерения многомерного свойства
    */
    public Long getDimMultiPropItem() {
    return this.dimMultiPropItem;
    }

    public void setDimMultiPropItem(Long v) {
    this.dimMultiPropItem = v;
    }

    /**
    * Факторы и значения факторов
    */
    public Long getFactor() {
    return this.factor;
    }

    public void setFactor(Long v) {
    this.factor = v;
    }

    /**
    * Формат визуализации
    */
    public Long getVisualFormat() {
    return this.visualFormat;
    }

    public void setVisualFormat(Long v) {
    this.visualFormat = v;
    }

    }
