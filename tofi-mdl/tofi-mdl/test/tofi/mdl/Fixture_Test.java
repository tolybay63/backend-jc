package tofi.mdl;

import jandcode.core.apx.test.Apx_Test;
import org.junit.jupiter.api.Test;

public class Fixture_Test extends Apx_Test {

    @Test
    public void updateFixtureSuite() throws Exception {
        dbm.updateFixtureSuite("testMeasure");
        dbm.updateFixtureSuite("testAttrib");
        dbm.updateFixtureSuite("testFactor");
        dbm.updateFixtureSuite("testMeter");
        dbm.updateFixtureSuite("testRole");
        dbm.updateFixtureSuite("testTyp");
    }


}
