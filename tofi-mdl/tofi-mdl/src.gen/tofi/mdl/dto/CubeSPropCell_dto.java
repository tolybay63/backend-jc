/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Ячейки прокуба свойств
* <p>
    * table: CubeSPropCell
    */
    public class CubeSPropCell_dto {

    private Long id;

    private Long cubeS;

    private Long prop;

    private Long propStatus;

    private Long propProvider;

    private Long multiProp;

    private Long multiPropStatus;

    private Long multiPropProvider;

    private Long measure;

    @FieldProps(dict = "FD_PropStatusMissing")
    private Long propStatusMissing;

    @FieldProps(dict = "FD_PropProviderMissing")
    private Long propProviderMissing;

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
    * Стандартные кубы
    */
    public Long getCubeS() {
    return this.cubeS;
    }

    public void setCubeS(Long v) {
    this.cubeS = v;
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
    * Статусы свойства
    */
    public Long getPropStatus() {
    return this.propStatus;
    }

    public void setPropStatus(Long v) {
    this.propStatus = v;
    }

    /**
    * Поставщик свойства
    */
    public Long getPropProvider() {
    return this.propProvider;
    }

    public void setPropProvider(Long v) {
    this.propProvider = v;
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
    * Статусы свойства
    */
    public Long getMultiPropStatus() {
    return this.multiPropStatus;
    }

    public void setMultiPropStatus(Long v) {
    this.multiPropStatus = v;
    }

    /**
    * Поставщики свойства
    */
    public Long getMultiPropProvider() {
    return this.multiPropProvider;
    }

    public void setMultiPropProvider(Long v) {
    this.multiPropProvider = v;
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
    * Выбор статуса при его отсутствии
    */
    public Long getPropStatusMissing() {
    return this.propStatusMissing;
    }

    public void setPropStatusMissing(Long v) {
    this.propStatusMissing = v;
    }

    /**
    * Выбор поставщика при его отсутствии
    */
    public Long getPropProviderMissing() {
    return this.propProviderMissing;
    }

    public void setPropProviderMissing(Long v) {
    this.propProviderMissing = v;
    }

    }
