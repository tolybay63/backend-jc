package dtj.report.dao;

import java.io.File;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class ClearService implements Runnable {
    String dir;

    public ClearService(String dir) {
        this.dir = dir;
    }

    @Override
    public void run() {
        // Создаем сервис с одним потоком для выполнения задачи
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

        // Определяем задачу (Runnable), которую нужно выполнять
        Runnable cleanupTask = new Runnable() {
            public void run() {
                System.out.println("Запуск задачи очистки в: " + new java.util.Date());
                cleanUpFiles(dir);
            }
        };

        // Планируем выполнение задачи:
        // 0L      -> начальная задержка (запускается немедленно при старте)
        // 1L      -> интервал между выполнениями
        // TimeUnit.HOURS -> единица измерения интервала (часы)
        scheduler.scheduleAtFixedRate(cleanupTask, 0L, 1L, TimeUnit.HOURS);
    }

    private static void cleanUpFiles(String directoryPath) {
        File dir = new File(directoryPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Директория не найдена или не является директорией: " + directoryPath);
            return;
        }

        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isFile() && ((new Date()).getTime() - file.lastModified()) / 1000 / 60 / 60 > 1) {
                    if (file.delete()) {
                        System.out.println("Удален файл: " + file.getName());
                    } else {
                        System.err.println("Не удалось удалить файл: " + file.getName());
                    }
                }
            }
        }
    }


}
