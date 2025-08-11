/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Свойства
* <p>
    * table: Prop
    */
    public class Prop_dto {

    private Long id;

    @FieldProps(dict = "FD_AccessLevel")
    private Long accessLevel;

    @FieldProps(size = 30)
    private String cod;

    private Long propGr;

    private Long parent;

    @FieldProps(dict = "FD_PropType")
    private Long propType;

    private Boolean isUniq;

    private Boolean isDependValueOnPeriod;

    private Boolean isDependNameOnPeriod;

    @FieldProps(size = 200)
    private String name;

    @FieldProps(size = 400)
    private String fullName;

    private Long attrib;

    private Long factor;

    private Long meter;

    private Long meterRate;

    private Long typ;

    private Long relTyp;

    private Boolean allItem;

    @FieldProps(dict = "FD_VisualFormat")
    private Long visualFormat;

    private Long statusFactor;

    private Long providerTyp;

    private Long ord;

    private Long measure;

    @FieldProps(dict = "FD_MeterBehavior")
    private Long meterBehavior;

    private Double minVal;

    private Double maxVal;

    private Integer digit;

    @FieldProps(size = 1000)
    private String propTag;

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
    * Уровень доступа
    */
    public Long getAccessLevel() {
    return this.accessLevel;
    }

    public void setAccessLevel(Long v) {
    this.accessLevel = v;
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
    * Группы свойств
    */
    public Long getPropGr() {
    return this.propGr;
    }

    public void setPropGr(Long v) {
    this.propGr = v;
    }

    /**
    * Свойства
    */
    public Long getParent() {
    return this.parent;
    }

    public void setParent(Long v) {
    this.parent = v;
    }

    /**
    * Тип свойства
    */
    public Long getPropType() {
    return this.propType;
    }

    public void setPropType(Long v) {
    this.propType = v;
    }

    /**
    * isUniq
    */
    public Boolean getIsUniq() {
    return this.isUniq;
    }

    public void setIsUniq(Boolean v) {
    this.isUniq = v;
    }

    /**
    * isDependValueOnPeriod
    */
    public Boolean getIsDependValueOnPeriod() {
    return this.isDependValueOnPeriod;
    }

    public void setIsDependValueOnPeriod(Boolean v) {
    this.isDependValueOnPeriod = v;
    }

    /**
    * isDependNameOnPeriod
    */
    public Boolean getIsDependNameOnPeriod() {
    return this.isDependNameOnPeriod;
    }

    public void setIsDependNameOnPeriod(Boolean v) {
    this.isDependNameOnPeriod = v;
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
    * Атрибутивные свойства
    */
    public Long getAttrib() {
    return this.attrib;
    }

    public void setAttrib(Long v) {
    this.attrib = v;
    }

    /**
    * Факторы и значения факторов
    */
    public Long getFactor() {
    return this.factor;
    }

    public void setFactor(Long v) {
    this.factor = v;
    }

    /**
    * Измерители
    */
    public Long getMeter() {
    return this.meter;
    }

    public void setMeter(Long v) {
    this.meter = v;
    }

    /**
    * Показатели
    */
    public Long getMeterRate() {
    return this.meterRate;
    }

    public void setMeterRate(Long v) {
    this.meterRate = v;
    }

    /**
    * Типы объектов
    */
    public Long getTyp() {
    return this.typ;
    }

    public void setTyp(Long v) {
    this.typ = v;
    }

    /**
    * Отношения между типами объектов
    */
    public Long getRelTyp() {
    return this.relTyp;
    }

    public void setRelTyp(Long v) {
    this.relTyp = v;
    }

    /**
    * allItem
    */
    public Boolean getAllItem() {
    return this.allItem;
    }

    public void setAllItem(Boolean v) {
    this.allItem = v;
    }

    /**
    * Формат визуализации
    */
    public Long getVisualFormat() {
    return this.visualFormat;
    }

    public void setVisualFormat(Long v) {
    this.visualFormat = v;
    }

    /**
    * Факторы и значения факторов
    */
    public Long getStatusFactor() {
    return this.statusFactor;
    }

    public void setStatusFactor(Long v) {
    this.statusFactor = v;
    }

    /**
    * Типы объектов
    */
    public Long getProviderTyp() {
    return this.providerTyp;
    }

    public void setProviderTyp(Long v) {
    this.providerTyp = v;
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
    * Единицы измерения
    */
    public Long getMeasure() {
    return this.measure;
    }

    public void setMeasure(Long v) {
    this.measure = v;
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
    * digit
    */
    public Integer getDigit() {
    return this.digit;
    }

    public void setDigit(Integer v) {
    this.digit = v;
    }

    /**
    * propTag
    */
    public String getPropTag() {
    return this.propTag;
    }

    public void setPropTag(String v) {
    this.propTag = v;
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
