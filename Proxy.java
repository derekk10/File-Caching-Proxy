import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.concurrent.ConcurrentHashMap;

class Proxy {
	protected static int EIO = -5;
	protected static int EACCES = -13;
	protected static int EFAULT = -14;
	static String serverip;
	static int port;
	static String cachedirectory;
	static long cachesize;
	static long currcachesize = 0;
	static RMIInterface server;

	//cache maps pathnames to files
	private static Map<String, Fwrapper> cache = new HashMap<String, Fwrapper>();
	//linked list of files in cache
	private static LinkedList<File> list = new LinkedList<File>();
	//cache maps pathnames to most recent file version;
	private static ConcurrentHashMap<String, File> versionMap = new ConcurrentHashMap<String, File>();
	
	private static class FileHandler implements FileHandling {
		private static int fd = 5;
		
		private Map<Integer, Fwrapper> hm = new HashMap<Integer, Fwrapper>();

		public int open( String path, OpenOption o ) {
			System.err.println("OPEN path: " + path + ", flag: " + o);
			fd++;
			//Open a file for read/write, create if it does not exist
			String mode = "rw";
			File f;
			Fwrapper file;
			String cachePath;
			//checks if path is valid 
			if (!isPathValid(path)) {
				System.err.println("EPERMS invalid path");
				return Errors.EPERM;
			}
			File mostRecent = versionMap.get(path);
			if (mostRecent != null) {
				System.err.println("are we here");
				cachePath = mostRecent.getPath();
				// cachePath = myPaths(filePath).toString();
			}
			else {
				System.err.println("or do we go here");
				//case where we are creating a new file since if the most recent
				// file is not in the cache, then it doesnt exist at all
				cachePath = myPaths(path).toString();
			}
			switch (o) {
				case CREATE: 
					try {
					//check if file exists on proxy
						if (cache.get(cachePath) == null) {
							System.err.println("file not in cache");
							//file not in cache
							//now check if file exists on server
							if (server.fileExists(path)) {
								//file is in server
								//fetch and store in cache
								byte[] fileContent = server.fetch(path);

								String versionId = server.getVersion(path);
								int objsize = fileContent.length;
								FileInfo fileInfo = server.getInfo(path);
								if (!fileInfo.canRead || !fileInfo.canWrite) {
									System.err.println("EACCES cant read or cant write");
									return EACCES;
								}
								//made a new version and stored it in cache
								file = version(cachePath, path, versionId);
								//write file contents into new file
								file.raf.write(fileContent);
								file.raf.seek(0);
							}
							else {
								//create new file on server and proxy
								//create file on server
								System.err.println("is this path making a directory? path: " + path);
								FileInfo fileInfo = server.createFile(path);
								if (!fileInfo.canRead || !fileInfo.canWrite) {
									System.err.println("EACCES cant read or cant write");
									return EACCES;
								}
								String versionId = server.getVersion(path);
								//create file in proxy and put in cache
								file = version(cachePath, path, versionId);
							}
						}
						else {
							System.err.println("file is in cache");
							//file is in cache but we want to check if its up to date
							String serverVersion = server.getVersion(path);
							String version = cache.get(cachePath).uuid;
							//if file on cache is not up to date
							if (!serverVersion.equals(version)) {
								System.err.println("Updating stale cache version");
								//create new version
								byte[] fileContent = server.fetch(path);
								file = version(cachePath, path, serverVersion);
								file.raf.write(fileContent);
								file.raf.seek(0);
							}
							else {
								System.err.println("retreiving filewrapper from cache");
								file = cache.get(cachePath);
								//reopen raf after closing in close()
								file.raf = new RandomAccessFile(file.f, file.mode);
							}
						}
					}
					catch (RemoteException e) {
						System.err.println("RemoteException");
						return EIO;
					}
					catch (NullPointerException e) {
						System.err.println("EFAULT server.createFile error");
						e.printStackTrace();
						return EFAULT;
					}
					catch (IOException e) {
						System.err.println("EIO .createNewFile exception");
						e.printStackTrace();
						return EIO;
					}
					file.incRefCount();
					hm.put(fd, file);
					System.err.println("open return fd:" + fd);
					return fd;
				//Create new file for read/write, returning error if it already exists
				case CREATE_NEW:
					//case where file in cache
					if (cache.get(cachePath) != null) {
						System.err.println("EEXIST file already exists");
						return Errors.EEXIST;
					}
					//file not in cache
					else {
						//case where file on server (then return error)
						try {
							if (server.fileExists(path)) {
								System.err.println("EEXIST file already exists");
								return Errors.EEXIST;
							}
						//file not on server
						//create file on server and proxy
							else {
								//create file on server
								FileInfo fileInfo = server.createFile(path);
								if (!fileInfo.canRead || !fileInfo.canWrite) {
									System.err.println("EACCES cant read or cant write");
									return EACCES;
								}
								//creates new versionId for server file
								String versionId = server.getVersion(path);
								//create file in proxy and put in cache
								file = version(cachePath, path, versionId);
							}
						}
						catch (IOException e) {
							System.err.println("EIO .createNewFile exception");
							return EIO;
						}
						catch (NullPointerException e) {
							System.err.println("EFAULT server.createFile error");
							e.printStackTrace();
							return EFAULT;
						}
					}
					file.incRefCount();
					hm.put(fd, file);
					System.err.println("open return fd: " + fd);
					return fd;
				//Open existing file or directory for read only
				case READ: 
					//file not in cache
					try {
						System.err.println("PATHTHH: " + cachePath);
						System.err.println("ogpath: " + path);
						if (cache.get(cachePath) == null){
							//file in server but not in cache so fetch and store
							if (server.fileExists(path)) {
								System.err.println("file in server but not in cache");
								byte[] fileContent = server.fetch(path);
								String versionId = server.getVersion(path);
								int objsize = fileContent.length;
								FileInfo fileInfo = server.getInfo(path);
								if (!fileInfo.canRead) {
									System.err.println("EACCES cant read or cant write");
									return EACCES;
								}
								file = version(cachePath, path, versionId);
								//write file contents into new file
								file.raf.write(fileContent);
								file.raf.seek(0);
							}
							else {
								System.err.println("ENOENT file doesnt exist");
								return Errors.ENOENT;
							}
						}
						//file in cache
						else {
							System.err.println("file in cache");
							FileInfo fileInfo = server.getInfo(path);
							if (!fileInfo.canRead) {
								System.err.println("EACCES cant read or cant write");
								return EACCES;
							}
							String serverVersion = server.getVersion(path);
							String version = cache.get(cachePath).uuid;
							//if file on cache is not up to date
							if (!serverVersion.equals(version)) {
								System.err.println("update cache");
								//create new version
								byte[] fileContent = server.fetch(path);
								file = version(cachePath, path, serverVersion);
								file.raf.write(fileContent);
								file.raf.seek(0);
							} else {
								System.err.println("retreiving file wrapper from cache");
								file = cache.get(cachePath);
								System.err.println("refCount: " + file.refCount);
								file.raf = new RandomAccessFile(file.f, file.mode);
							}
						}
					}
					catch (RemoteException e) {
						System.err.println("EIO RemoteException err");
						e.printStackTrace();
						return EIO;
					}
					catch (IOException e) {
						System.err.println("EIO .createNewFile exception");
						e.printStackTrace();
						return EIO;
					}
					catch (NullPointerException e) {
						System.err.println("EFAULT server.createFile error");
						e.printStackTrace();
						return EFAULT;
					}
					file.incRefCount();
					hm.put(fd, file);
						
					System.err.println("open return fd: " + fd);
					return fd;			
				case WRITE:
					//file not in cache
					try {
						if (cache.get(cachePath) == null) {
							//file not in cache but exists in server so fetch
							if (server.fileExists(path)) {
								byte[] fileContent = server.fetch(path);
								String versionId = server.getVersion(path);
								FileInfo fileInfo = server.getInfo(path);
								if (fileInfo.isDir) {
									System.err.println("EISDIR file is directory");
									return Errors.EISDIR;
								}
								if (!fileInfo.canWrite) {
									System.err.println("EACCES cant read or cant write");
									return EACCES;
								}
								file = version(cachePath, path, versionId);
								file.raf.write(fileContent);
								file.raf.seek(0);
							}
							else {
								System.err.println("ENOENT file doesnt exist");
								return Errors.ENOENT;
							}
						}
					//file in cache but we have to check if stale
						else {
							FileInfo fileInfo = server.getInfo(path);
							if (fileInfo.isDir) {
								System.err.println("EISDIR file is directory");
								return Errors.EISDIR;
							}
							if (!fileInfo.canWrite) {
								System.err.println("EACCES cant read or cant write");
								return EACCES;
							}
							//not updated
							String serverVersion = server.getVersion(path);
							String version = cache.get(cachePath).uuid;
							if (!serverVersion.equals(version)) {
								//create new version
								byte[] fileContent = server.fetch(path);
								file = version(cachePath, path, serverVersion);
								file.raf.write(fileContent);
								file.raf.seek(0);
							}
							else {
								file = cache.get(cachePath);
								file.raf = new RandomAccessFile(file.f, file.mode);
							}
						}
					}
					catch (RemoteException e) {
						System.err.println("EIO RemoteException");
						return EIO;
					}
					catch (IOException e) {
						System.err.println("EIO .createNewFile exception");
						return EIO;
					}
					catch (NullPointerException e) {
						System.err.println("EFAULT server.createFile error");
						e.printStackTrace();
						return EFAULT;
					}
					file.incRefCount();
					hm.put(fd, file);
					System.err.println("open return fd: " + fd);
					return fd;
				default:
					System.err.println("EINVAL");
					return Errors.EINVAL;
			}
		}

		public int close( int fd ) {
			System.err.println("CLOSE fd: " + fd);
			if (fd < 0) {
				System.err.println("EINVAL fd < 0");
				return Errors.EINVAL;
			}
			try {
				Fwrapper file = hm.get(fd);
				System.err.println(file.f.getPath());
				if (file == null) {
					System.err.println("EBADF file wrapper null");
					return Errors.EBADF;
				}
				if (!file.f.isDirectory()) {
					if (versionMap.get(file.path) == null) {
						versionMap.put(file.path, file.f);
					}
					byte[] fileContent = getFileContent(file.raf);
					if (file.dirtybit) {
						
						//update server here according to session semantics
						server.updateServer(file.path, fileContent);
						// String serverVersion = server.getVersion(file.path);
						// String newPath = file.path + serverVersion;
						// File newFile = new File(newPath);
						// file.f.renameTo(newFile);
						server.updateServerVersion(file.path, file.uuid);
						versionMap.replace(file.path, file.f);
						System.err.println("updated server");
						file.dirtybit = false;
						//move file to end of linked list since it was just accessed
					}
					
					if (file.uuid.equals(server.getVersion(file.path))) {
						//first instance of file 
						if (cache.get(file.f.getPath()) == null) {
							long objsize = fileContent.length;
							evict(objsize);
							list.add(file.f);
							currcachesize += objsize;
						}
						else {
							list.remove(file.f);
							list.add(file.f);
						}
					}
					else {
						long objsize = fileContent.length;
						evict(objsize);
						list.add(file.f);
						currcachesize += objsize;
						System.err.println("current size of cache: " + currcachesize);	
					}
					cache.put(file.f.getPath(), file);
				}
				if (file.raf != null) {
						file.raf.close();
				}
				else {
					System.err.println("raf is null");
				}
				
				hm.remove(fd);
				file.decRefCount();
			}
			catch (RemoteException e) {
				System.err.println("EIO RemoteException");
				return EIO;
			}
			catch (NullPointerException e) {
				System.err.println("EFAULT server.createFile error");
				e.printStackTrace();
				return EFAULT;
			}
			catch (FileNotFoundException e) {
				System.err.println("EFAULT FileNotFoundException");
				e.printStackTrace();
				return EFAULT;
			}
			catch (IOException e) {
				System.err.println("EIO IOException");
				e.printStackTrace();
				return EIO;
			}
			System.err.println("closed correctly");
			printCache();
			return 0;
		}

		public long write( int fd, byte[] buf ) {
			System.err.println("WRITE fd: " + fd);
			if (fd < 0) {
				System.err.println("EINVAL fd < 0");
				return Errors.EINVAL;
			}
			if (buf == null) {
				System.err.println("EFAULT buf is null");
				return EFAULT;
			}
			Fwrapper file = hm.get(fd);
			System.err.println("path of fd: " + file.path);
			if (file == null) {
				System.err.println("EBADF file wrapper null");
				return Errors.EBADF;
			}
			if (!file.mode.equals("rw")) {
				System.err.println("EBADF not rw mode");
				return Errors.EBADF;
			}
			long bytes = buf.length;
			//make a new version to avoid concurrency issues
			try {
				RandomAccessFile raf = file.raf;
				byte[] oldContent = getFileContent(raf);
				Long fp = raf.getFilePointer();
				String version = UUID.randomUUID().toString();
				Path oldVersion = Paths.get(file.f.getPath());
				Path newVersion = myPaths(file.path + version);
				Files.copy(oldVersion, newVersion);
				File copy = new File(newVersion.toString());
				Fwrapper newfw = new Fwrapper(copy, file.path, 1);
				newfw.dirtybit = true;
				newfw.uuid = version;
				newfw.raf.write(oldContent);
				newfw.raf.seek(fp);
				newfw.raf.write(buf);
				hm.put(fd, newfw);
				file.decRefCount();
				file.raf.close();
				

				
				// byte[] fileContent = getFileContent(file);
				// String serverVersion = server.getVersion(file.path);
				// String cachePath = myPaths(file.path).toString();
				// Long fp = file.raf.getFilePointer();
				// file.raf.close();
				// Fwrapper newVersion = version(cachePath, file.path, serverVersion);
				// newVersion.incRefCount();
				// newVersion.raf.seek(0);
				// newVersion.raf.write(fileContent);
				// newVersion.raf.seek(fp);
				// newVersion.raf.write(buf);
				// newVersion.path = file.path;
				// newVersion.dirtybit = true;
				// hm.put(fd, newVersion);
			}
			catch (IOException e) {
				System.err.println("EIO .write exception");
				e.printStackTrace();
				return EIO;
			}
			System.err.println("write return: " + bytes);
			return bytes;
		}

		public long read( int fd, byte[] buf ) {
			System.err.println("READ fd: " + fd);
			if (fd < 0) {
				System.err.println("EINVAL fd < 0");
				return Errors.EINVAL;
			}
			if (buf == null) {
				System.err.println("EFAULT buf is null");
				return EFAULT;
			}
			Fwrapper file = hm.get(fd);
			System.err.println(file.f.getPath());
			if (file == null) {
				System.err.println("EBADF file null");
				return Errors.EBADF;
			}
			if (file.f.isDirectory()) {
				System.err.println("EISDIR file is directory");
				return Errors.EISDIR;
			}
			try {
				int bytes = file.raf.read(buf);
				if (bytes == -1) {
					bytes = 0;
				}
				System.err.println("read return: " + bytes);
				return bytes;
			}
			catch (IOException e) {
				System.err.println("EIO .read exception");
				e.printStackTrace();
				return EIO;
			}
		}

		public long lseek( int fd, long pos, LseekOption o ) {
			System.err.println("LSEEK fd: " + fd + "pos: " + pos + "flag: " + o);
			Fwrapper file = hm.get(fd);
			if (file == null) {
				System.err.println("EBADF null file wrapper");
				return Errors.EBADF;
			}
			long updatePos = 0;
			switch (o) {
				case FROM_CURRENT:
					try {
						updatePos = file.raf.getFilePointer() + pos;
						file.raf.seek(updatePos);
						System.err.println("updatePos: " + updatePos);
						if (updatePos < 0) {
							System.err.println("EINVAL updatePos: " + updatePos);
							return Errors.EINVAL;
						}
					}
					catch (IOException e) {
						System.err.println("EINVAL");
						return Errors.EINVAL;
					}
					break;
				case FROM_END:
					try {
						updatePos = file.raf.length() - pos;
						file.raf.seek(updatePos);
						if (updatePos < 0) {
							System.err.println("EINVAL updatePos: " + updatePos);
							return Errors.EINVAL;
						}
					}
					catch (IOException e) {
						System.err.println("EINVAL");
						return Errors.EINVAL;
					}
					break;
				case FROM_START:
					try {
						System.err.println("HERE FUCK");
						updatePos = pos;
						file.raf.seek(updatePos);
						if (updatePos < 0) {
							System.err.println("EINVAL updatePos: " + updatePos);
							return Errors.EINVAL;
						}
					}
					catch (IOException e) {
						System.err.println("EINVAL");
						return Errors.EINVAL;
					}
					break;
			}
			try {
				file.raf.seek(updatePos);
			}
			catch (IOException e) {
				return Errors.EINVAL;
			}System.err.println("lseek returned correctly with: " + updatePos);
			return updatePos;
		}

		public int unlink( String path ) {
			System.err.println("UNLINK path: " + path);
			String cachePath = myPaths(path).toString();
			try {
				//file doesnt exist on server
				if (!server.fileExists(path)) {
					System.err.println("ENOENT file doesnt exist");
					return Errors.ENOENT;
				}
				//file exists
				else {
					//delete from server
					server.unlink(path);
					//check if file in cache
					// if (cache.get(cachePath) != null) {
					// 	//delete from cache
					// 	File f = cache.get(path);
					// 	cache.remove(path);
					// 	f.delete();
					// }
				}
			}
			catch (RemoteException e) {
				System.err.println("EIO fileExists error");
				return EIO;
			}
			catch (NullPointerException e) {
				System.err.println("EFAULT fileExists error");
				return EFAULT;
			}
			//file exists on server
			System.err.println("Unlinked correctly");
			return 0;
		}

		public void clientdone() {
			for (Map.Entry<Integer, Fwrapper> entry : hm.entrySet()) {
				int key = entry.getKey();
				Fwrapper file = hm.get(key);
				try {
					if (file.raf != null) {
						file.raf.close();
						System.err.println("client done closed correctly");
					}
					else {
						System.err.println("raf is null");
					}
				}
				catch (IOException e) {
					System.err.println("error closing rafs");
				}
			return;
			}
		}
	}

	public static Path myPaths(String pathString) {
		Path canonicalPath;
		Path path = Paths.get(pathString);
		if (path.isAbsolute()) {
			canonicalPath = path;
		}
		else {
			canonicalPath = Paths.get(cachedirectory, pathString);
		}
		return canonicalPath.normalize();
	}

	public static byte[] getFileContent(RandomAccessFile raf) throws FileNotFoundException, RemoteException, NullPointerException, IOException{
		System.err.println("getFileContent");
		long fp = raf.getFilePointer();
		raf.seek(0L);
		long len = raf.length();
		byte[] buffer = new byte[(int)len];
		raf.readFully(buffer);
		raf.seek(fp);
		return buffer;
	}

	public static Fwrapper version(String path, String ogpath, String versionId) throws NullPointerException, FileNotFoundException, IOException{
		System.err.println("making new version");
		//put new file in cachedirectory and hashmap
		//put file in cache directory
		String newPath = path + versionId;
		File f = new File(newPath);
		File parent;
		if ((parent = f.getParentFile()) != null) {
			//file has parent directories
			if (!parent.exists()) {
				//create directories if they dont exist
				parent.mkdirs();
			}
		}
		System.err.println("path: " + path);
		f.createNewFile();
		System.err.println("is a directory?: " + f.isDirectory());
		Fwrapper file = new Fwrapper(f, ogpath, 0);
		file.uuid = versionId;
		//put file in hashmap
		System.err.println("ref count: " + file.refCount);

		System.err.println("PATH THAT WE ARE CACHING: " + newPath);
		return file;
	} 
	
	public static boolean isPathValid(String path) {
		Path newPath = myPaths(path);
		Path cachedir = Paths.get(cachedirectory);
		if (!newPath.startsWith(cachedir)) {
			return false;
		}
		return true;
	}
	public static void evict(long objsize) {
		int i = 0;
		File f;
		while (currcachesize + objsize > cachesize) {
			//need to check if file to be evicted is still open
			for (File iterator: list) {
				String path = iterator.getPath(); //wrong path
				System.err.println("file we are checking: " + path);
				Fwrapper file = cache.get(path);
				if (file == null) {
					System.err.println("file is null");
				}
				System.err.println("file we are checking: " + file.f.getPath());
				if (file.refCount == 0) {
					f = file.f;
					list.remove(f);
					System.err.println("path of element being evicted: " + path);
					cache.remove(path);
					currcachesize -= f.length();
					f.delete();
					System.err.println("evicting" + f.getName() + "from cache");
					i++;
					break;
				}
			}
		}
		if (i > 0) {
			System.err.println("evicted " + i + " element(s) from the cache");
		}
		return;
	}
	public static void printCache() {
		System.err.println("-----Files in cache-----");
		for (File iterator: list) {
			String path = iterator.getPath();
			System.err.println(path);
		}
		System.err.println("------------------------");
	}
	
	private static class FileHandlingFactory implements FileHandlingMaking {
		public FileHandling newclient() {
			return new FileHandler();
		}
	}

	public static void main(String[] args) throws IOException {
		System.err.println("Hello World");
		serverip = args[0];
		port = Integer.parseInt(args[1]);
		cachedirectory = args[2];
		cachesize = Long.parseLong(args[3]);
		File cacheDir = new File(cachedirectory);
		cacheDir.mkdirs();
		try {
			Registry registry = LocateRegistry.getRegistry(serverip, port);
			server = (RMIInterface)registry.lookup("RMIInterface");
		}
		catch (RemoteException e) {
			System.err.println("Unable to locate registry or unable to use RMIInterface");
			e.printStackTrace();
			System.exit(1);
		}
		catch (NotBoundException e) {
			System.err.println("Interface not found");
			e.printStackTrace();
			System.exit(1);
		}
		(new RPCreceiver(new FileHandlingFactory())).run();
	}
}

