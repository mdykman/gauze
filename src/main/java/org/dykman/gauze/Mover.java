package org.dykman.gauze;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.viafoura.common.cassandra.AbstractClusterPeer;
import com.viafoura.common.cassandra.SimpleClusterPeer;

public class Mover {

    static class CassandraInstance {
	AbstractClusterPeer peer;
	Session session;
	String keyspace;

	public CassandraInstance(AbstractClusterPeer peer, Session session,
	    String keyspace) {
	    this.peer = peer;
	    this.session = session;
	    this.keyspace = keyspace;

	}

	public void close() {
	    peer.destroy();
	}

	public List<String> getSchema(String sourceKeyspace,
	    String destKeyspace, Collection<String> tables) {
	    Cluster cluster = peer.getCluster();
	    Metadata meta = cluster.getMetadata();
	    KeyspaceMetadata kmeta = meta.getKeyspace(keyspace);
	    Collection<TableMetadata> tmeta = kmeta.getTables();
//	    List<String> drops = new ArrayList<>();
	    List<String> created = new ArrayList<>();

	    for (TableMetadata tab : tmeta) {
		if (tables.size() != 0) {
		    String name = tab.getName();
		    if (!tables.contains(name))
			continue;
		}
		String s = tab.asCQLQuery();
		StringBuilder sb = new StringBuilder();
		sb.append("DROP TABLE IF EXISTS ")
			.append(destKeyspace).append(".").
			append(tab.getName()).append(";\n");
		
		created.add(sb.toString());
		created.add(s.replaceAll("CREATE TABLE " + sourceKeyspace,
		    "CREATE TABLE " + destKeyspace));
	    }
	    return created;
	}

	public List<String> getTables() throws ExecutionException {
	    String query = "select columnfamily_name from system.schema_columnfamilies where keyspace_name = ?";
	    List<String> tables = new ArrayList<String>();
	    BoundStatement bs = peer.bPrepare(session, query);
	    bs.setString(0, keyspace);
	    ResultSet rs = session.execute(bs);
	    for (Row row : rs) {
		String s = row.getString(0);
		tables.add(s);
	    }

	    return tables;
	}

	public List<ListenableFuture<ResultSet>> applyStatements(List<String> ss,boolean sync) 
	throws Exception {
	    List<ListenableFuture<ResultSet>> list = new ArrayList<>();
	    for (String s : ss) {
		System.out.println(s);
		if(sync) {
		    session.execute(s);
		} else {
		    list.add(session.executeAsync(s));
		}
	    }
	    return list;
	}
    }

    CassandraInstance source;
    CassandraInstance destination;

    public Mover(CassandraInstance src, CassandraInstance dest) {
	this.source = src;
	this.destination = dest;
    }

    

    public List<ListenableFuture<ResultSet>> setDestinationSchema(List<String> ss) 
    	throws Exception {
	return destination.applyStatements(ss,true);
    }

    public List<String> getSourceSchema(String sourceKeyspace,
	String destKeyspace, Collection<String> tables) {
	return source.getSchema(sourceKeyspace, destKeyspace, tables);
    }

    public void close() {
	source.close();
	destination.close();
    }

    public List<String> getSourceTables() throws ExecutionException {
	return source.getTables();
    }

    public List<ListenableFuture<ResultSet>> moveTable(String name) throws ExecutionException {

	System.out.println("inserting data to destination:" + name);
	String query = "select * from " + name;
	BoundStatement bs = source.peer.bPrepare(source.session, query);
	ResultSet rs = source.session.execute(bs);
	ColumnDefinitions defs = rs.getColumnDefinitions();
	int ncols = defs.size();
	boolean update = false;
	for (int i = 0; i < ncols; ++i) {
	    DataType dt = defs.getType(i);
	    if (DataType.Name.COUNTER.equals(dt.getName())) {
		update = true;
		break;
	    }
	}
	if (update) {
	    return moveTableUpdate(name, rs, defs);
	} else {
	    return moveTableInsert(name, rs, defs);
	}
    }

    public List<ListenableFuture<ResultSet>> moveTableUpdate(String name, ResultSet sourceResult,
	ColumnDefinitions defs) throws ExecutionException {
	int ncols = defs.size();

	StringBuilder body = new StringBuilder("update " + name)
	    .append(" SET ");
	boolean nb = true;
	boolean ns = true;
	List<ListenableFuture<ResultSet>> list = new ArrayList<>();
	StringBuilder spec = new StringBuilder(" WHERE ");
	for (int i = 0; i < ncols; ++i) {
	    String cn = defs.getName(i);
	    boolean isRowKey = name == null ? false
		: (cn.startsWith("row_") || cn.startsWith("cluster_"));
	    if (isRowKey) {
		if (ns) {
		    ns = false;
		} else {
		    spec.append(" AND ");
		}
		spec.append(cn + " = ?");
	    } else {
		if (nb) {
		    nb = false;
		} else {
		    body.append(", ");
		}
		DataType dt = defs.getType(i);
		if (!DataType.Name.COUNTER.equals(dt.getName())) {
			body.append(cn + " = ?");
		} else {
		    body.append(cn + " = " + cn + " + ?");
		}

	    }
	}
	body.append(spec.toString());
	System.out.println(body.toString());

	BoundStatement export = destination.peer.bPrepare(destination.session,
	    body.toString());
	// Row row;
	int counter = 0;
	for (Row row : sourceResult) {
	    for (int i = 0; i < ncols; ++i) {
		export.setBytesUnsafe(i, row.getBytesUnsafe(i));
	    }
	    list.add(destination.session.executeAsync(export));
	    ++counter;
	}
	System.out.println("" + counter + " rows applied");
	return list;
    }

    public List<ListenableFuture<ResultSet>> moveTableInsert(String name, ResultSet sourceResult,
	ColumnDefinitions defs) throws ExecutionException {
	List<ListenableFuture<ResultSet>> list = new ArrayList<>();
	int ncols = defs.size();

	StringBuilder builder = new StringBuilder("insert into " + name)
	    .append(" (");
	for (int i = 0; i < ncols; ++i) {
	    if (i > 0)
		builder.append(",");
	    builder.append(defs.getName(i));
	}

	builder.append(") values(");
	for (int i = 0; i < ncols; ++i) {
	    if (i > 0)
		builder.append(",");
	    builder.append("?");
	}
	builder.append(")");
	// System.out.println(builder.toString());
	BoundStatement export = destination.peer.bPrepare(destination.session,
	    builder.toString());
	// Row row;
	int counter = 0;
	for (Row row : sourceResult) {
	    for (int i = 0; i < ncols; ++i) {
		export.setBytesUnsafe(i, row.getBytesUnsafe(i));
	    }
	    list.add(destination.session.executeAsync(export));
	    ++counter;
	}
	System.out.println("" + counter + " rows applied");
	return list;
    }

    static Options options = new Options();
    static {
	options.addOption("s", false, "copy schema");
	options.addOption("h", false, "print help");
    }

    public static void usage() {
	System.out
	    .println("java "
		+ Mover.class.getName()
		+ " [-s] source-seed source-keyspace dest-seed dest-keyspace [table ...]");
	System.out.println();
	System.out
	    .println("If no tables are specified, the entire keyspace is copied.");
	System.out
	    .println("Applying -c will create the target keyspace if one does not already exist");
    }

    public static void main(String[] args) {
	CommandLineParser parser = new BasicParser();
	Mover mover = null;
	try {
	    CommandLine command = parser.parse(options, args, true);
	    boolean cschema = command.hasOption("s");
	    boolean help = command.hasOption("h");
	    if (help) {
		usage();
		System.exit(0);
	    }

	    args = command.getArgs();
	    if (args.length < 4) {
		System.err.println("insufficient parameters");
		usage();
		System.exit(0);
	    }

	    String sh = args[0];
	    String sk = args[1];
	    String sd = args[2];
	    String dk = args[3];

	    if (args.length > 4) {
		args = Arrays.copyOfRange(args, 4, args.length);
	    } else {
		args = new String[] {};
	    }

	    SimpleClusterPeer source = new SimpleClusterPeer(sh);
	    SimpleClusterPeer dest = new SimpleClusterPeer(sd);
	    Session ssrc = source.getSession(sk);

	    CassandraInstance sourceInstance = new CassandraInstance(source,
		ssrc, sk);

	    System.out.println("using source " + sh + "|" + sk + ", dest " + sd
		+ "|" + dk);

	    Session sdst = null;
	    try {
		sdst = dest.getSession(dk);
	    } catch (NoHostAvailableException e) {
		System.err.println("unable to reach destination database");
		return;
	    } catch(InvalidQueryException e) {
		System.out.println("Destination keyspace " + dk 
		    + " does not exist.  Attempting to create");

		sdst = dest.getSession();
		Metadata smd = source.getCluster().getMetadata();
		KeyspaceMetadata skmeta = smd.getKeyspace(sk);
		sdst.execute(skmeta.asCQLQuery().replace(sk, dk));
		sdst.close();
		sdst = dest.getSession(dk);
	    }

	    mover = new Mover(sourceInstance, new CassandraInstance(dest, sdst,
		dk));

	    List<String> tables = new ArrayList<>();
	    for (String a : args) {
		tables.add(a);
	    }
	    if (tables.size() == 0) {
		tables = mover.getSourceTables();
	    }

	    System.out.println("retrieving schema from source");
	    List<String> ls = mover.getSourceSchema(sk, dk, tables);
	    System.out.println("applying schema to destination");
	    List<ListenableFuture<ResultSet>> list = mover.setDestinationSchema(ls);
	    ListenableFuture<List<ResultSet>> f = Futures.allAsList(list);
	    f.get();
	    System.out.println("done schema");

	    List<ListenableFuture<ResultSet>> all = new ArrayList<>();
	    for (String aa : tables) {
		System.out.println("processing table " + aa);
		try {
		    List<ListenableFuture<ResultSet>> set = mover.moveTable(aa);
		    all.addAll(set);
		} catch (InvalidQueryException e) {
		    System.out.println();
		    System.err
			.println("!! invalid query while processing table "
			    + aa + ": " + e.getLocalizedMessage());
		    System.out.println();
		} catch (Exception e) {
		    System.out.println();
		    System.err.print("!! exception while processing table "
			+ aa + ": ");
		    System.err.println("terminating");
		    return;
		}
	    }
	    f = Futures.allAsList(all);
	    f.get();
	    System.out.println("done");
	} catch (Exception e) {
	    e.printStackTrace();
	} finally {
	    if (mover != null) {
		mover.close();
	    }
	}
    }
}
