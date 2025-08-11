/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.datetime.*;
import jandcode.commons.reflect.*;

/**
 * Измерения стандартного куба
* <p>
    * table: CubeSDim
    */
    public class CubeSDim_dto {

    private Long id;

    private Long cubeS;

    @FieldProps(dict = "FD_CubeSDimType")
    private Long cubeSDimType;

    private Long dimPeriod;

    private Long dimProp;

    private Long dimObj;

    private XDate dOrg;

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
    * Тип измерения куба
    */
    public Long getCubeSDimType() {
    return this.cubeSDimType;
    }

    public void setCubeSDimType(Long v) {
    this.cubeSDimType = v;
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
    * Измерения свойств
    */
    public Long getDimProp() {
    return this.dimProp;
    }

    public void setDimProp(Long v) {
    this.dimProp = v;
    }

    /**
    * Измерения объектов и отношений
    */
    public Long getDimObj() {
    return this.dimObj;
    }

    public void setDimObj(Long v) {
    this.dimObj = v;
    }

    /**
    * dOrg
    */
    public XDate getDOrg() {
    return this.dOrg;
    }

    public void setDOrg(XDate v) {
    this.dOrg = v;
    }

    }
