import java.io.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;

public class FileInfo implements Serializable {
    boolean isDir;
    boolean canRead;
    boolean canWrite;
    public FileInfo(File file) {
        isDir = file.isDirectory();
        canRead = file.canRead();
        canWrite = file.canWrite();
    }
}