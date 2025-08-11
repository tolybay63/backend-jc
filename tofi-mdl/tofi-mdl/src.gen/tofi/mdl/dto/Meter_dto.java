/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Измерители
* <p>
    * table: Meter
    */
    public class Meter_dto {

    private Long id;

    @FieldProps(size = 30)
    private String cod;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    private Long measure;

    @FieldProps(dict = "FD_MeterStruct")
    private Long meterStruct;

    @FieldProps(dict = "FD_MeterDeterm")
    private Long meterDeterm;

    @FieldProps(dict = "FD_DistributionLaw")
    private Long distributionLaw;

    @FieldProps(dict = "FD_MeterType")
    private Long meterTypeByRate;

    @FieldProps(dict = "FD_MeterType")
    private Long meterTypeByPeriod;

    @FieldProps(dict = "FD_MeterType")
    private Long meterTypeByMember;

    @FieldProps(dict = "FD_MeterBehavior")
    private Long meterBehavior;

    private Double minVal;

    private Double maxVal;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private String cmt;

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
    * Код
    */
    public String getCod() {
    return this.cod;
    }

    public void setCod(String v) {
    this.cod = v;
    }

    /**
    * Уровень доступа
    */
    public Long getAccessLevel() {
    return this.accessLevel;
    }

    public void setAccessLevel(Long v) {
    this.accessLevel = v;
    }

    /**
    * Единицы измерения
    */
    public Long getMeasure() {
    return this.measure;
    }

    public void setMeasure(Long v) {
    this.measure = v;
    }

    /**
    * Структура измерителя
    */
    public Long getMeterStruct() {
    return this.meterStruct;
    }

    public void setMeterStruct(Long v) {
    this.meterStruct = v;
    }

    /**
    * Детерминированность измерителя
    */
    public Long getMeterDeterm() {
    return this.meterDeterm;
    }

    public void setMeterDeterm(Long v) {
    this.meterDeterm = v;
    }

    /**
    * Закон распределения
    */
    public Long getDistributionLaw() {
    return this.distributionLaw;
    }

    public void setDistributionLaw(Long v) {
    this.distributionLaw = v;
    }

    /**
    * Тип измерителя
    */
    public Long getMeterTypeByRate() {
    return this.meterTypeByRate;
    }

    public void setMeterTypeByRate(Long v) {
    this.meterTypeByRate = v;
    }

    /**
    * Тип измерителя
    */
    public Long getMeterTypeByPeriod() {
    return this.meterTypeByPeriod;
    }

    public void setMeterTypeByPeriod(Long v) {
    this.meterTypeByPeriod = v;
    }

    /**
    * Тип измерителя
    */
    public Long getMeterTypeByMember() {
    return this.meterTypeByMember;
    }

    public void setMeterTypeByMember(Long v) {
    this.meterTypeByMember = v;
    }

    /**
    * Поведение измерителя
    */
    public Long getMeterBehavior() {
    return this.meterBehavior;
    }

    public void setMeterBehavior(Long v) {
    this.meterBehavior = v;
    }

    /**
    * minVal
    */
    public Double getMinVal() {
    return this.minVal;
    }

    public void setMinVal(Double v) {
    this.minVal = v;
    }

    /**
    * maxVal
    */
    public Double getMaxVal() {
    return this.maxVal;
    }

    public void setMaxVal(Double v) {
    this.maxVal = v;
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

    /**
    * Комментарий
    */
    public String getCmt() {
    return this.cmt;
    }

    public void setCmt(String v) {
    this.cmt = v;
    }

    }
