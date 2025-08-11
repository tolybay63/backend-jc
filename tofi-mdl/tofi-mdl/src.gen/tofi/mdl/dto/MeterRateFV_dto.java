/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Значения факторов, от которых зависит показатель мягкого измерителя
* <p>
    * table: MeterRateFV
    */
    public class MeterRateFV_dto {

    private Long id;

    private Long meterRate;

    private Long factorVal;

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
    * Показатели
    */
    public Long getMeterRate() {
    return this.meterRate;
    }

    public void setMeterRate(Long v) {
    this.meterRate = v;
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

    }
