/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Возможные значения элемента измерения многомерного свойства типа отношение между объектами
* <p>
    * table: DimMultiPropItemRelCls
    */
    public class DimMultiPropItemRelCls_dto {

    private Long id;

    private Long dimMultiPropItem;

    private Long relCls;

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
    * Классы отношений
    */
    public Long getRelCls() {
    return this.relCls;
    }

    public void setRelCls(Long v) {
    this.relCls = v;
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
