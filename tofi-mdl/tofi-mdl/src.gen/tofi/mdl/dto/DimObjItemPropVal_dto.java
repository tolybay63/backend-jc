/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;
import jandcode.commons.reflect.*;

/**
 * Значение свойства для класса или отношения между классами
* <p>
    * table: DimObjItemPropVal
    */
    public class DimObjItemPropVal_dto {

    private Long id;

    private Long dimObjItemProp;

    private Double numberVal;

    @FieldProps(size = 1000)
    private String strVal;

    private String multiStrVal;

    private XDateTime dateTimeVal;

    private Long propVal;

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
    * Свойства, которые определяют значение или владельца
    */
    public Long getDimObjItemProp() {
    return this.dimObjItemProp;
    }

    public void setDimObjItemProp(Long v) {
    this.dimObjItemProp = v;
    }

    /**
    * numberVal
    */
    public Double getNumberVal() {
    return this.numberVal;
    }

    public void setNumberVal(Double v) {
    this.numberVal = v;
    }

    /**
    * strVal
    */
    public String getStrVal() {
    return this.strVal;
    }

    public void setStrVal(String v) {
    this.strVal = v;
    }

    /**
    * multiStrVal
    */
    public String getMultiStrVal() {
    return this.multiStrVal;
    }

    public void setMultiStrVal(String v) {
    this.multiStrVal = v;
    }

    /**
    * dateTimeVal
    */
    public XDateTime getDateTimeVal() {
    return this.dateTimeVal;
    }

    public void setDateTimeVal(XDateTime v) {
    this.dateTimeVal = v;
    }

    /**
    * Возможные значения ссылочных свойств
    */
    public Long getPropVal() {
    return this.propVal;
    }

    public void setPropVal(Long v) {
    this.propVal = v;
    }

    }
