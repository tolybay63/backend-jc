package tofi.apinator;

import jandcode.core.*;

import java.util.*;

public interface ApinatorExcludeMethodsService extends Comp {

    /**
     * Набор имен методов, которые исключаются из api
     */
    Set<String> getMethods();

}
