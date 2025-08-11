/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;

import jandcode.commons.reflect.*;

/**
 * Отношения между типам объектов
* <p>
    * table: CubeSFaceValue
    */
    public class CubeSFaceValue_dto {

    private Long id;

    private Long cubeSFace;

    private Long relTyp;

    private Long relCls;

    @FieldProps(size = 4000)
    private String dimObjItemComb;

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
    * Грани прокуба объектов и отношений
    */
    public Long getCubeSFace() {
    return this.cubeSFace;
    }

    public void setCubeSFace(Long v) {
    this.cubeSFace = v;
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
    * Классы отношений
    */
    public Long getRelCls() {
    return this.relCls;
    }

    public void setRelCls(Long v) {
    this.relCls = v;
    }

    /**
    * dimObjItemComb
    */
    public String getDimObjItemComb() {
    return this.dimObjItemComb;
    }

    public void setDimObjItemComb(String v) {
    this.dimObjItemComb = v;
    }

    }
