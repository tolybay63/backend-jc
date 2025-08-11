/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Кластерные факторы
* <p>
    * table: TypClusterFactor
    */
    public class TypClusterFactor_dto {

    private Long id;

    private Long typ;

    private Long factor;

    private Boolean isReq;

    private Boolean isUniq;

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
    * Типы объектов
    */
    public Long getTyp() {
    return this.typ;
    }

    public void setTyp(Long v) {
    this.typ = v;
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
    * Обязательность кластерного фактора 
    */
    public Boolean getIsReq() {
    return this.isReq;
    }

    public void setIsReq(Boolean v) {
    this.isReq = v;
    }

    /**
    * Однозначность кластерного фактора 
    */
    public Boolean getIsUniq() {
    return this.isUniq;
    }

    public void setIsUniq(Boolean v) {
    this.isUniq = v;
    }

    }
