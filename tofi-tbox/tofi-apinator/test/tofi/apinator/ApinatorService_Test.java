package tofi.apinator;

import jandcode.core.test.*;
import org.junit.jupiter.api.*;

public class ApinatorService_Test extends App_Test {

    @Test
    public void test1() throws Exception {
        ApinatorService svc = app.bean(ApinatorService.class);

        for (var api : svc.getApis()) {
            utils.delim();
            System.out.println(api.getName());
            System.out.println();

            for (var item : api.getItems()) {
                System.out.println(item.getName());
                System.out.println("---> " + item);
                for (var m : item.getMethods()) {
                    System.out.println("        " + m.getName() + " -> " + m.getMethod());
                }
            }

        }

    }


}
