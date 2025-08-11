/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Свойства, входящие в характеристическую группу
* <p>
    * table: RelTypCharGrProp
    */
    public class RelTypCharGrProp_dto {

    private Long id;

    private Long relTypCharGr;

    private Long prop;

    private Long relTypCharGrProp_measure;

    private Long propVal_measure;

    private Long multiProp;

    @FieldProps(dict = "FD_StorageType")
    private Long storageType;

    private Long flatTable;

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
    * Характеристические группы отношений типов объектов
    */
    public Long getRelTypCharGr() {
    return this.relTypCharGr;
    }

    public void setRelTypCharGr(Long v) {
    this.relTypCharGr = v;
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
    * Свойства
    */
    public Long getRelTypCharGrProp_measure() {
    return this.relTypCharGrProp_measure;
    }

    public void setRelTypCharGrProp_measure(Long v) {
    this.relTypCharGrProp_measure = v;
    }

    /**
    * Возможные значения ссылочных свойств
    */
    public Long getPropVal_measure() {
    return this.propVal_measure;
    }

    public void setPropVal_measure(Long v) {
    this.propVal_measure = v;
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
    * БД хранения значений свойства
    */
    public Long getStorageType() {
    return this.storageType;
    }

    public void setStorageType(Long v) {
    this.storageType = v;
    }

    /**
    * Плоские таблицы
    */
    public Long getFlatTable() {
    return this.flatTable;
    }

    public void setFlatTable(Long v) {
    this.flatTable = v;
    }

    }
