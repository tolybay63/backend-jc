package dtj.report.dao;

import jandcode.core.dbm.mdb.BaseMdbUtils;
import jandcode.core.dbm.mdb.Mdb;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
public class FileCleanupService extends BaseMdbUtils {
    Mdb mdb;
    FileCleanupService(Mdb mdb) {
        this.mdb = mdb;
    }

    public void go() {
        final String dir = mdb.getApp().getAppdir() + File.separator + "reports";

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

        System.out.println("Сервис очистки запущен. Ожидание первого выполнения...");

        // Приложение будет работать в фоне, выполняя задачу каждый час.
        // Чтобы остановить приложение, вам нужно будет закрыть JVM или добавить логику graceful shutdown.
    }

    /**
     * Метод для удаления файлов в заданной директории.
     */
    private static void cleanUpFiles(String directoryPath) {
        File dir = new File(directoryPath);
        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Директория не найдена или не является директорией: " + directoryPath);
            return;
        }

        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                // Пример фильтрации: удаляем только файлы с расширением .tmp
                if (file.isFile() && file.getName().toLowerCase().endsWith(".tmp")) {
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
