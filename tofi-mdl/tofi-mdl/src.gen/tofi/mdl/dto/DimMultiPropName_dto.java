/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Дополнительные столбцы измерения
* <p>
    * table: DimMultiPropName
    */
    public class DimMultiPropName_dto {

    private Long id;

    private Long DimMultiProp;

    @FieldProps(size = 200)
    private String title;

    private Long ord;

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
    * Измерения многомерного свойства
    */
    public Long getDimMultiProp() {
    return this.DimMultiProp;
    }

    public void setDimMultiProp(Long v) {
    this.DimMultiProp = v;
    }

    /**
    * Заголовок
    */
    public String getTitle() {
    return this.title;
    }

    public void setTitle(String v) {
    this.title = v;
    }

    /**
    * Порядковый номер
    */
    public Long getOrd() {
    return this.ord;
    }

    public void setOrd(Long v) {
    this.ord = v;
    }

    }
