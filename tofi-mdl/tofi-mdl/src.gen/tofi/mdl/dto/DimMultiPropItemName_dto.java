/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Дополнительные названия элементов измерения
* <p>
    * table: DimMultiPropItemName
    */
    public class DimMultiPropItemName_dto {

    private Long id;

    private Long DimMultiPropItem;

    private Long DimMultiPropName;

    @FieldProps(size = 200)
    private String name;

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
    return this.DimMultiPropItem;
    }

    public void setDimMultiPropItem(Long v) {
    this.DimMultiPropItem = v;
    }

    /**
    * Дополнительные столбцы измерения
    */
    public Long getDimMultiPropName() {
    return this.DimMultiPropName;
    }

    public void setDimMultiPropName(Long v) {
    this.DimMultiPropName = v;
    }

    /**
    * Наименование
    */
    public String getName() {
    return this.name;
    }

    public void setName(String v) {
    this.name = v;
    }

    }
