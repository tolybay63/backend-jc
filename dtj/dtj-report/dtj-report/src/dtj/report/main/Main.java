package dtj.report.main;

import jandcode.commons.cli.*;
import jandcode.core.cli.*;
import jandcode.core.web.cli.*;
import jandcode.core.apx.cli.*;

public class Main {

    public static void main(String[] args) {
        new Main().run(args);
    }

    public void run(String[] args) {
        CliLauncher z = new CliLauncher(args);
        z.addExtension(new AppCliExtension());
        z.addCmd("serve", new ServeCliCmd());
        z.addCmd("db-check", new DbCheckCliCmd());
        z.addCmd("db-create", new DbCreateCliCmd());
        z.exec();
    }

}
