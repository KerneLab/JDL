package org.kernelab.jdl;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;

public class LoadWorker implements Runnable
{
	private static int			ID			= 0;

	private final int			id			= ID++;

	private LoadMaster			master;

	private Connection			connection;

	private Thread				thread;

	private boolean				stopping	= false;

	private boolean				running		= false;

	private PreparedStatement	statement;

	private LinkedList<Record>	records		= new LinkedList<Record>();

	private int					batchSize	= 1000;

	private LinkedList<Record>	bads		= new LinkedList<Record>();

	public LoadWorker(LoadMaster master, Connection conn)
	{
		this.setMaster(master).setConnection(conn);
	}

	protected void addBatch(PreparedStatement ps, int[] index, String[] data) throws SQLException
	{
		for (int i = 0; i < index.length; i++)
		{
			ps.setString(i + 1, data[index[i]]);
		}
		ps.addBatch();
	}

	protected int addUpdate(PreparedStatement ps, int[] index, String[] data) throws SQLException
	{
		for (int i = 0; i < index.length; i++)
		{
			ps.setString(i + 1, data[index[i]]);
		}
		return ps.executeUpdate();
	}

	protected int[] checkResult(int[] results, LinkedList<Record> records, LinkedList<Record> bads)
	{
		int[] result = new int[] { records.size(), 0 };
		int i = 0, res = -1;
		bads.clear();
		Record record = null;
		while (!records.isEmpty())
		{
			record = records.removeFirst();
			res = results[i];
			if (res == Statement.EXECUTE_FAILED)
			{
				bads.add(record);
			}
			i++;
		}
		result[1] = this.trackBads(this.getConnection(), this.getStatement(), this.getTemplate().getIndexes(), bads);
		return result;
	}

	public void destroy()
	{
		if (this.getMaster() != null)
		{
			this.getMaster().reportDestroy(this);
			this.setMaster(null);
		}
		this.setStatement(null);
		try
		{
			this.getConnection().close();
			this.setConnection(null);
		}
		catch (Exception e)
		{
		}
		this.getRecords().clear();
		this.getBads().clear();
		this.setThread(null);
	}

	protected int[] doBatch(Connection conn, PreparedStatement ps, int[] index, LinkedList<Record> records)
	{
		int total = records.size();
		try
		{
			conn.setAutoCommit(false);

			ps.clearBatch();

			for (Record record : records)
			{
				this.addBatch(ps, index, record.data);
			}

			int[] res = ps.executeBatch();

			conn.commit();

			return this.checkResult(res, records, this.getBads());
		}
		catch (BatchUpdateException e)
		{
			try
			{
				conn.rollback();
			}
			catch (SQLException ex)
			{
				ex.printStackTrace();
			}
			return this.checkResult(e.getUpdateCounts(), records, this.getBads());
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			return new int[] { total, total };
		}
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
		{
			return true;
		}
		if (obj == null)
		{
			return false;
		}
		if (getClass() != obj.getClass())
		{
			return false;
		}
		LoadWorker other = (LoadWorker) obj;
		if (id != other.id)
		{
			return false;
		}
		return true;
	}

	protected LinkedList<Record> getBads()
	{
		return bads;
	}

	public int getBatchSize()
	{
		return batchSize;
	}

	protected Connection getConnection()
	{
		return connection;
	}

	protected LoadMaster getMaster()
	{
		return master;
	}

	protected LinkedList<Record> getRecords()
	{
		return records;
	}

	protected PreparedStatement getStatement()
	{
		return statement;
	}

	protected InsertTemplate getTemplate()
	{
		return this.getMaster().getTemplate();
	}

	public Thread getThread()
	{
		return thread;
	}

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	protected void init() throws SQLException
	{
		if (this.getStatement() == null)
		{
			this.getConnection().setAutoCommit(false);
			this.setStatement(this.getConnection().prepareStatement(this.getTemplate().getInsert()));
		}
	}

	public synchronized boolean isRunning()
	{
		return running;
	}

	public synchronized boolean isStopped()
	{
		return this.getThread() == null;
	}

	public synchronized boolean isStopping()
	{
		return stopping;
	}

	protected void logBad(Record record, Exception ex)
	{
		record.printError(System.err, ex);
	}

	protected void readRecords(BlockingQueue<Record> queue, List<Record> records)
	{
		records.clear();
		Record rec = null;
		for (int i = 0; i < this.getBatchSize(); i++)
		{
			try
			{
				rec = queue.take();
				if (rec == RecordParser.END)
				{
					break;
				}
				else
				{
					records.add(rec);
				}
			}
			catch (InterruptedException e)
			{
			}
		}
	}

	@Override
	public void run()
	{
		try
		{
			this.init();

			while (!this.isStopping())
			{
				synchronized (this)
				{
					this.setRunning(true);
				}

				this.readRecords(this.getMaster().getRecordsQueue(), this.getRecords());

				this.getMaster().reportRead(this);

				int[] result = this.doBatch(this.getConnection(), this.getStatement(), this.getTemplate().getIndexes(),
						this.getRecords());

				this.getMaster().reportLoaded(this, result[0], result[1]);

				synchronized (this)
				{
					this.setRunning(false);
					if (!this.isStopping())
					{
						try
						{
							this.wait();
						}
						catch (InterruptedException e)
						{
						}
					}
				}
			}
		}
		catch (SQLException ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			synchronized (this)
			{
				this.destroy();
				this.notifyAll();
			}
		}
	}

	protected LoadWorker setBads(LinkedList<Record> bads)
	{
		this.bads = bads;
		return this;
	}

	public LoadWorker setBatchSize(int batchSize)
	{
		this.batchSize = Math.max(batchSize, 1);
		return this;
	}

	protected LoadWorker setConnection(Connection connection)
	{
		this.connection = connection;
		return this;
	}

	protected LoadWorker setMaster(LoadMaster master)
	{
		this.master = master;
		return this;
	}

	protected LoadWorker setRecords(LinkedList<Record> records)
	{
		this.records = records;
		return this;
	}

	protected LoadWorker setRunning(boolean running)
	{
		this.running = running;
		return this;
	}

	protected void setStatement(PreparedStatement statement)
	{
		this.statement = statement;
	}

	protected LoadWorker setStopping(boolean stopping)
	{
		this.stopping = stopping;
		return this;
	}

	protected LoadWorker setThread(Thread thread)
	{
		this.thread = thread;
		return this;
	}

	public synchronized Thread start()
	{
		Thread t = this.getThread();
		if (t != null)
		{
			return t;
		}
		t = new Thread(this);
		this.setThread(t);
		t.start();
		return t;
	}

	public synchronized void stop()
	{
		Thread t = this.getThread();
		if (t != null)
		{
			this.setStopping(true);
			this.notifyAll();
		}
	}

	protected int trackBads(Connection conn, PreparedStatement ps, int[] index, LinkedList<Record> bads)
	{
		int total = bads.size();
		try
		{
			conn.setAutoCommit(true);

			int bad = 0;

			Record rec = null;
			while (!bads.isEmpty())
			{
				rec = bads.removeFirst();
				try
				{
					this.addUpdate(ps, index, rec.data);
				}
				catch (SQLException err)
				{
					bad++;
					this.logBad(rec, err);
				}
			}

			return bad;
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			return total;
		}
	}

	public synchronized void wakeup()
	{
		Thread t = this.getThread();
		if (t != null)
		{
			this.notifyAll();
		}
	}
}
