/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Тип атрибутивного значения элемента измерения многомерного свойства
* <p>
    * table: DimMultiPropItemAttrib
    */
    public class DimMultiPropItemAttrib_dto {

    private Long id;

    private Long dimMultiPropItem;

    @FieldProps(dict = "FD_AttribValType")
    private Long attribValType;

    @FieldProps(size = 60)
    private String format;

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
    * Значения атрибута
    */
    public Long getAttribValType() {
    return this.attribValType;
    }

    public void setAttribValType(Long v) {
    this.attribValType = v;
    }

    /**
    * format
    */
    public String getFormat() {
    return this.format;
    }

    public void setFormat(String v) {
    this.format = v;
    }

    }
