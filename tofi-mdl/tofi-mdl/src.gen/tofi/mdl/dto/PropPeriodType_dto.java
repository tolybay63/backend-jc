/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Типы периодов
* <p>
    * table: PropPeriodType
    */
    public class PropPeriodType_dto {

    private Long id;

    private Long prop;

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
    * Свойства
    */
    public Long getProp() {
    return this.prop;
    }

    public void setProp(Long v) {
    this.prop = v;
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
