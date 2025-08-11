/* THIS FILE GENERATED! NOT MODIFY! */
package tofi.mdl.dto;


/**
 * Измерения грани прокуба объектов и отношений
* <p>
    * table: CubeSFaceDim
    */
    public class CubeSFaceDim_dto {

    private Long id;

    private Long cubeSFace;

    private Long dimObj;

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
    * Измерения объектов и отношений
    */
    public Long getDimObj() {
    return this.dimObj;
    }

    public void setDimObj(Long v) {
    this.dimObj = v;
    }

    }
