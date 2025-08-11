/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Настройки значений шкалы
* <p>
    * table: ScaleValAsgn
    */
    public class ScaleValAsgn_dto {

    private Long id;

    private Long scaleVal;

    private Long scaleAsgn;

    private Double scaleValNumber;

    @FieldProps(size = 16)
    private String scaleValColor;

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
    * Значения шкалы
    */
    public Long getScaleVal() {
    return this.scaleVal;
    }

    public void setScaleVal(Long v) {
    this.scaleVal = v;
    }

    /**
    * Настройки шкалы
    */
    public Long getScaleAsgn() {
    return this.scaleAsgn;
    }

    public void setScaleAsgn(Long v) {
    this.scaleAsgn = v;
    }

    /**
    * Числовое значение значения шкалы
    */
    public Double getScaleValNumber() {
    return this.scaleValNumber;
    }

    public void setScaleValNumber(Double v) {
    this.scaleValNumber = v;
    }

    /**
    * Цвет значения шкалы
    */
    public String getScaleValColor() {
    return this.scaleValColor;
    }

    public void setScaleValColor(String v) {
    this.scaleValColor = v;
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
