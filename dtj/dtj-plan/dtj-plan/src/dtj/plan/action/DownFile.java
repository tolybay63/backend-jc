package dtj.plan.action;

import java.io.File;

public class DownFile {

    private final File file;
    private final String clientName;

    public DownFile(File file, String clientName) {
        this.file = file;
        this.clientName = clientName;
    }

    public File getFile() {
        return file;
    }

    public String getClientName() {
        return clientName;
    }

}
