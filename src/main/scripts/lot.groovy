


class Lot {

	Map<String,List<Attendant> > interest = new java.util.concurrent.ConcurrentLinkedHashMap<String, List<Attendant> >();

	Lot(pfile) {
      props = loadProperties(args[0]);
		String host = props.get("host")
		if(!host) {
			host = "0.0.0.0"
		}
		String s = props.get("port")
		Integer port = new Integer(s) 
		server = new ServerSocket(port)
println "server created, at port ${port}"

	}
	def run() {
		while(true) {
			println "accepting"
			Socket socket = server.accept();
			new Attendant(socket).run()
		}
	}

	def disInterest(String topic, Attendant attendant) {
		List<Attendant> tt = interest.get(topic)
		if(tt != null) {
			tt.remove(attendant)
		}
	}

	def showInterest(String topic, Attendant attendant) {
		List<Attendant> tt = interest.get(topic)
		if(tt == null) {
			tt = new ArrayList<Attendant> ()
			interest.put(tt)
		}
		tt.add(attendant)
	}

}


abstract class Runner implements Callable<Object> {
	CompleteListener warden;
	public Runner(CompleteListener l) {
		this.warden = l
	}
	public Object call() {
		try {
			warden.onComplete(doIt(),null)
		} catch(Exception e) {
			warden.onComplete(null,e)
		}
		
	}
	public abstract Object doIt();
}
interface CompleteListener {
	public void onComplete(Object result,Throwable thr);
}

class Attendant  implements CompleteListener {
	def label;
	Lot lot
	Socket socket

	BufferedInputStream bin;
	OutputStream out;
	List<CompleteListener> listeners = new ArrayList<CompleteListener>()

	int counter = 1
	Attendant(Lot lot,Socket sock,String label) {
		this.lot = lot
		this.socket = sock
		this.label = label

		bin = new BufferedInputStream(sock.inputStream)
		out = sock.outputStream
	}

	public void onComplete(Object result,Throwable thr) {
	}

	def complete() {
	}
}

class __Attendant {
	
	Lot lot
	Socket socket
	BufferedInputStream bin;
	OutputStream out;
	List<String> topics = new ArrayList<String>();
	__Attendant(Lot lot,Socket sock) {
		this.lot = lot
		this.socket = sock
		bin = new BufferedInputStream(sock.inputStream)
		out = sock.outputStream
	}
	def write(String text) {
		out.write(text)
	}
	def afterrun() {
		for(String s: topics) {
			lot.disInterest(s,this)
		}
	}

	def run() {
		if(beforerun()) {
			midrun()
			afterrun()
		}
	}

	def midrun() {
	}
	def beforerun() {
		String line;
		while(line = bin.readLine()) {
			if(line == null) { 
				// connection broken, client is gone
				return false;
			}
			if(line.size() == 0) {
				break;
			}
			topics.add(line)
			lot.showInterest(line,this)
		}
		true
	}
}






















Properties loadProperties(name) {
	Properties props = new Properties();
	props.load new File(name).newInputStream()
	props
}
