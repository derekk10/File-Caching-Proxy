import java.rmi.Remote;
import java.rmi.RemoteException;
import java.io.*;


interface RMIInterface extends Remote {
    public FileInfo createFile(String path) throws RemoteException, NullPointerException, IOException;
    public FileInfo getInfo(String path) throws RemoteException, NullPointerException;
    public int updateServer(String path, byte[] fileContent) throws RemoteException, NullPointerException, IllegalArgumentException, FileNotFoundException, IOException;
    public boolean fileExists(String path) throws RemoteException, NullPointerException;
    public byte[] fetch(String path) throws RemoteException, NullPointerException, FileNotFoundException, IOException;
    public int unlink (String path) throws RemoteException;
    public String getVersion(String path) throws RemoteException;
    public void updateServerVersion(String path, String version) throws RemoteException;
}