/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Возможные значения элемента измерения многомерного свойства типа единица измерения
* <p>
    * table: DimMultiPropItemMeasure
    */
    public class DimMultiPropItemMeasure_dto {

    private Long id;

    private Long dimMultiPropItem;

    private Long measure;

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
    * Единицы измерения
    */
    public Long getMeasure() {
    return this.measure;
    }

    public void setMeasure(Long v) {
    this.measure = v;
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
