/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Статусы свойства
* <p>
    * table: MultiPropStatus
    */
    public class MultiPropStatus_dto {

    private Long id;

    private Long multiProp;

    private Long factorVal;

    private Boolean isDefault;

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
    public Long getFactorVal() {
    return this.factorVal;
    }

    public void setFactorVal(Long v) {
    this.factorVal = v;
    }

    /**
    * isDefault
    */
    public Boolean getIsDefault() {
    return this.isDefault;
    }

    public void setIsDefault(Boolean v) {
    this.isDefault = v;
    }

    }
