/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Типы периодов для многомерных свойств
* <p>
    * table: MultiPropPeriodType
    */
    public class MultiPropPeriodType_dto {

    private Long id;

    private Long multiProp;

    @FieldProps(dict = "FD_PeriodType")
    private Long periodType;

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
    * Многомерные свойства
    */
    public Long getMultiProp() {
    return this.multiProp;
    }

    public void setMultiProp(Long v) {
    this.multiProp = v;
    }

    /**
    * Типы периода
    */
    public Long getPeriodType() {
    return this.periodType;
    }

    public void setPeriodType(Long v) {
    this.periodType = v;
    }

    }
