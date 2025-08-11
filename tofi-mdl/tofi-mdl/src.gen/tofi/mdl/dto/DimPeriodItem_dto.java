/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;
import jandcode.commons.reflect.*;

/**
 * Элементы измерения периодов
* <p>
    * table: DimPeriodItem
    */
    public class DimPeriodItem_dto {

    private Long id;

    private Long dimPeriod;

    @FieldProps(dict = "FD_PeriodType")
    private Long periodType;

    private Long parent;

    @FieldProps(dict = "FD_PeriodNameTml")
    private Long periodNameTml;

    @FieldProps(dict = "FD_PeriodIncludeTag")
    private Long periodIncludeTag;

    private Long ord;

    private XDate dbeg;

    private Integer countPeriod;

    private XDate dend;

    private Integer lagCurrentDate;

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
    * Измерение периодов
    */
    public Long getDimPeriod() {
    return this.dimPeriod;
    }

    public void setDimPeriod(Long v) {
    this.dimPeriod = v;
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
    * Элементы измерения периодов
    */
    public Long getParent() {
    return this.parent;
    }

    public void setParent(Long v) {
    this.parent = v;
    }

    /**
    * Шаблон наименования периодов
    */
    public Long getPeriodNameTml() {
    return this.periodNameTml;
    }

    public void setPeriodNameTml(Long v) {
    this.periodNameTml = v;
    }

    /**
    * Признак включения периода в родительский период
    */
    public Long getPeriodIncludeTag() {
    return this.periodIncludeTag;
    }

    public void setPeriodIncludeTag(Long v) {
    this.periodIncludeTag = v;
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

    /**
    * Начало интервала
    */
    public XDate getDbeg() {
    return this.dbeg;
    }

    public void setDbeg(XDate v) {
    this.dbeg = v;
    }

    /**
    * countPeriod
    */
    public Integer getCountPeriod() {
    return this.countPeriod;
    }

    public void setCountPeriod(Integer v) {
    this.countPeriod = v;
    }

    /**
    * Конец интервала
    */
    public XDate getDend() {
    return this.dend;
    }

    public void setDend(XDate v) {
    this.dend = v;
    }

    /**
    * lagCurrentDate
    */
    public Integer getLagCurrentDate() {
    return this.lagCurrentDate;
    }

    public void setLagCurrentDate(Integer v) {
    this.lagCurrentDate = v;
    }

    }
