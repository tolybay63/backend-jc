/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Значения шкалы
* <p>
    * table: ScaleVal
    */
    public class ScaleVal_dto {

    private Long id;

    private Long scale;

    private Double minVal;

    private Boolean isMinValOpen;

    private Double maxVal;

    private Boolean isMaxValOpen;

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
    * Шкалы
    */
    public Long getScale() {
    return this.scale;
    }

    public void setScale(Long v) {
    this.scale = v;
    }

    /**
    * Нижняя граница значения шкалы
    */
    public Double getMinVal() {
    return this.minVal;
    }

    public void setMinVal(Double v) {
    this.minVal = v;
    }

    /**
    * Открытость нижней границы значения шкалы
    */
    public Boolean getIsMinValOpen() {
    return this.isMinValOpen;
    }

    public void setIsMinValOpen(Boolean v) {
    this.isMinValOpen = v;
    }

    /**
    * Верхняя граница значения шкалы
    */
    public Double getMaxVal() {
    return this.maxVal;
    }

    public void setMaxVal(Double v) {
    this.maxVal = v;
    }

    /**
    * Открытость верхней границы значения шкалы
    */
    public Boolean getIsMaxValOpen() {
    return this.isMaxValOpen;
    }

    public void setIsMaxValOpen(Boolean v) {
    this.isMaxValOpen = v;
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
