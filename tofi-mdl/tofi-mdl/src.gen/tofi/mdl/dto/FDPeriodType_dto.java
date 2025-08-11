/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Типы периода
* <p>
    * table: FD_PeriodType
    */
    public class FDPeriodType_dto {

    private Long id;

    @FieldProps(size = 200)
    private String text;

    private Boolean vis;

    private Long ord;

    @FieldProps(size = 20)
    private String code;

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
    * Наименование
    */
    public String getText() {
    return this.text;
    }

    public void setText(String v) {
    this.text = v;
    }

    /**
    * Видимость
    */
    public Boolean getVis() {
    return this.vis;
    }

    public void setVis(Boolean v) {
    this.vis = v;
    }

    /**
    * Порядок
    */
    public Long getOrd() {
    return this.ord;
    }

    public void setOrd(Long v) {
    this.ord = v;
    }

    /**
    * Код
    */
    public String getCode() {
    return this.code;
    }

    public void setCode(String v) {
    this.code = v;
    }

    }
