/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Наименование свойства зависит от периода
* <p>
    * table: PropNameOnPeriod
    */
    public class PropNameOnPeriod_dto {

    private Long id;

    private Long prop;

    @FieldProps(dict = "FD_PeriodType")
    private Long periodType;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

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

    /**
    * Краткое наименование
    */
    public String getName() {
    return this.name;
    }

    public void setName(String v) {
    this.name = v;
    }

    /**
    * Полное наименование
    */
    public String getFullName() {
    return this.fullName;
    }

    public void setFullName(String v) {
    this.fullName = v;
    }

    }
