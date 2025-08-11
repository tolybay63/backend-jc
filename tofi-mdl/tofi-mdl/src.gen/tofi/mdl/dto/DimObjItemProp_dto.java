/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Свойства, которые определяют значение или владельца
* <p>
    * table: DimObjItemProp
    */
    public class DimObjItemProp_dto {

    private Long id;

    private Long dimObjItem;

    private Long prop;

    private Long propStatus;

    @FieldProps(dict = "FD_PropStatusMissing")
    private Long propStatusMissing;

    private Long propProvider;

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
    * Компоненты измерения объектов и отношений
    */
    public Long getDimObjItem() {
    return this.dimObjItem;
    }

    public void setDimObjItem(Long v) {
    this.dimObjItem = v;
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
    * Выбор статуса при его отсутствии
    */
    public Long getPropStatusMissing() {
    return this.propStatusMissing;
    }

    public void setPropStatusMissing(Long v) {
    this.propStatusMissing = v;
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
    * Выбор поставщика при его отсутствии
    */
    public Long getPropProviderMissing() {
    return this.propProviderMissing;
    }

    public void setPropProviderMissing(Long v) {
    this.propProviderMissing = v;
    }

    }
