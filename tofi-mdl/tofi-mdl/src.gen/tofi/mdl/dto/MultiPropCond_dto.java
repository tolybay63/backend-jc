/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Внешние условия
* <p>
    * table: MultiPropCond
    */
    public class MultiPropCond_dto {

    private Long id;

    private Long multiProp;

    private Long factor;

    private Long typ;

    private Long relTyp;

    private Boolean isReq;

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
    * Многомерные свойства
    */
    public Long getMultiProp() {
    return this.multiProp;
    }

    public void setMultiProp(Long v) {
    this.multiProp = v;
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
    * isReq
    */
    public Boolean getIsReq() {
    return this.isReq;
    }

    public void setIsReq(Boolean v) {
    this.isReq = v;
    }

    }
