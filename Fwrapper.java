import java.io.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;

class Fwrapper implements Serializable {
    File f;
    RandomAccessFile raf;
    String mode;
    boolean dirtybit;
    String path;
    int refCount;
    String uuid;
    public Fwrapper(File file, String path) {
        f = file;
        mode = "rw";
        dirtybit = false;
        this.path = path;
        if (!f.isDirectory()) {
            try {
                raf = new RandomAccessFile(f, mode);
            }
            catch (FileNotFoundException e) {
                System.err.println("raf error");
            }
        }
    }
    public Fwrapper(File file, String path, int refCount) {
        f = file;
        mode = "rw";
        dirtybit = false;
        this.path = path;
        refCount = refCount;
        if (!f.isDirectory()) {
            try {
                raf = new RandomAccessFile(f, mode);
            }
            catch (FileNotFoundException e) {
                System.err.println("raf error");
            }
        }
    }
    public long getSize() { return f.length(); }
    public synchronized void incRefCount() { refCount++;}
    public synchronized void decRefCount() { refCount--;}
    public synchronized int getRefCount() { return refCount;}
}