import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.Naming;
import java.util.concurrent.locks.ReentrantReadWriteLock;
public class Server extends UnicastRemoteObject implements RMIInterface {
    static int port;
    static String rootdir;
    private static Map<String, String> versionMap = new HashMap<String, String>();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Server(int port) throws RemoteException {
        //port that server listens on
        //super's constructor creates remote object that listens on port
        super(port);
    }

    public FileInfo createFile(String path) throws RemoteException, NullPointerException, IOException {
        System.err.println("creating server file");
        String newPath = myPaths(path).toString();
        System.err.println("trying to create a file with this path FOR FUCKS SAKE: " + newPath);
        File f = new File(newPath);
        // String parents = f.getParent();
        // if (parents != null) {
        //     f.mkdirs();
        // }
        f.createNewFile();
        FileInfo file = new FileInfo(f);
        return file;
    }
    public FileInfo getInfo(String path) throws RemoteException, NullPointerException {
        System.err.println("Getting server file info");
        String newPath = myPaths(path).toString();
        File f = new File(newPath);
        FileInfo file = new FileInfo(f);
        return file;
    }
    public int updateServer(String path, byte[] fileContent) throws RemoteException, NullPointerException, IllegalArgumentException, FileNotFoundException, IOException {
        lock.writeLock().lock();
        System.err.println("updating server file");
        String newPath = myPaths(path).toString();
        System.err.println("path of file we are updating on server: " + newPath);
        File f = new File(newPath);
        RandomAccessFile raf = new RandomAccessFile(f, "rw");
        //clear the contents of the raf and then overwrite it
        raf.setLength(0L);
        raf.write(fileContent);
        raf.close();
        lock.writeLock().unlock();
        return 0;
    }

    public void updateServerVersion(String path, String version) {
        versionMap.put(path, version);
        return;
    }

    public String getVersion(String path) throws RemoteException {
        //if server file doesnt have a version then make it
        String versionId;
        if (versionMap.get(path) == null) {
            UUID id = UUID.randomUUID();
            versionId = id.toString();
            versionMap.put(path, versionId);
        }
        //otherwise get it from the versionMap and send it back to the client
        else {
            versionId = versionMap.get(path);
        }
        return versionId;
    }
    public boolean fileExists(String path) throws RemoteException, NullPointerException {
        String newPath = myPaths(path).toString();
        File f = new File(newPath);
        System.err.println("does server file exist?: " + f.exists());
        return f.exists();
    }
    public byte[] fetch(String path) throws RemoteException, NullPointerException, FileNotFoundException, IOException {
        lock.readLock().lock();
        System.err.println("server Fetch");
        String newPath = myPaths(path).toString();
        File f = new File(newPath);
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        long len = raf.length();
        byte[] buffer = new byte[(int)len];
        // long chunksize = 2048; // 2kB
        // byte[] buffer = new byte[(int)chunksize];
        raf.read(buffer);
        lock.readLock().unlock();
		return buffer;
    }
    public int unlink (String path) throws RemoteException{
        System.err.println("server.UNLINK path: " + path);
        String newPath = myPaths(path).toString();
        File f = new File(newPath);
        f.delete();
        return 0;
    }

    public static Path myPaths(String pathString) {
		Path canonicalPath;
		Path path = Paths.get(pathString);
		if (path.isAbsolute()) {
			canonicalPath = path;
		}
		else {
			canonicalPath = Paths.get(rootdir, pathString);
		}
		return canonicalPath.normalize();
	}
    public static void main(String args[]) {
        System.err.println("Server starting ...");
        port = Integer.parseInt(args[0]);
        rootdir = args[1];
        try {
            Server server = new Server(port);
            //create registry that listens at same port
            Registry registry = LocateRegistry.createRegistry(port);
            //bind exported remote object in registry
            registry.bind("RMIInterface", server);
            System.err.println("Server ready");
        }
        catch (Exception e) {
            System.err.println("Server exception");
            e.printStackTrace();
        }
    }
}
