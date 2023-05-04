package org.kernelab.jdl;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.kernelab.basis.Tools;

public class LoadWorker implements Runnable
{
	private static int			ID			= 0;

	private final int			id			= ID++;

	protected final Lock		lock		= new ReentrantLock();

	protected final Condition	notStarted	= lock.newCondition();

	// protected final Condition untilWaiting = lock.newCondition();

	protected final Condition	untilWakeup	= lock.newCondition();

	private LoadMaster			master;

	private Connection			connection;

	private Thread				thread;

	// private boolean waiting = false;

	private boolean				needWait	= true;

	private boolean				stopping	= false;

	private boolean				started		= false;

	private PreparedStatement	statement;

	private LinkedList<Record>	records		= new LinkedList<Record>();

	private int					batchSize	= 1000;

	private LinkedList<Record>	bads		= new LinkedList<Record>();

	// private boolean running = false;

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

	protected void destroy()
	{
		LoadMaster master = null;

		lock.lock();
		try
		{
			master = this.getMaster();
			this.setMaster(null);
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
		finally
		{
			lock.unlock();
		}

		if (master != null)
		{
			master.reportDestroy(this);
		}
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

	public int getId()
	{
		return id;
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

	protected boolean isNeedWait()
	{
		return needWait;
	}

	protected boolean isStarted()
	{
		return started;
	}

	protected boolean isStopping()
	{
		return stopping;
	}

	protected void log(String log)
	{
		Tools.debug(log);
	}

	// protected boolean isRunning()
	// {
	// return running;
	// }

	// protected boolean isWaiting()
	// {
	// return waiting;
	// }

	// protected boolean isStopped()
	// {
	// return this.getThread() == null;
	// }

	protected void logBad(Record record, Exception ex)
	{
		record.printError(this.getMaster().getErr(), ex);
	}

	protected int readRecords(RecordParser parser, List<Record> records)
	{
		int bads = 0;
		records.clear();
		Record record = null;
		int need = this.getTemplate().getItems().length;
		for (int i = 0; i < this.getBatchSize(); i++)
		{
			if (parser.hasNext())
			{
				record = parser.next();
				if (record.data.length != need)
				{
					bads++;
					this.logBad(record,
							new SQLException(record.data.length + " columns found but need " + need + " to load"));
				}
				else
				{
					records.add(record);
				}
			}
			else
			{
				break;
			}
		}
		return bads;
	}

	@Override
	public void run()
	{
		try
		{
			this.init();

			int badReads = 0;

			while (true)
			{
				lock.lock();
				try
				{
					if (!this.isStarted())
					{
						this.setStarted(true);
						notStarted.signalAll();
					}

					while (!this.isStopping() && this.isNeedWait())
					{
						// this.setWaiting(true);
						// untilWaiting.signalAll();
						try
						{
							untilWakeup.await();
						}
						catch (InterruptedException e)
						{
						}
					}

					// this.setWaiting(false);

					if (this.isStopping())
					{
						break;
					}

					this.setNeedWait(true);
				}
				finally
				{
					lock.unlock();
				}

				// synchronized (this)
				// {
				// if (!this.isStarted())
				// {
				// this.setStarted(true);
				// this.notifyAll();
				// }
				// // this.setRunning(false);
				// if (!this.isStopping())
				// {
				// // log("Worker#" + this.getId() + " waiting");
				// try
				// {
				// this.wait();
				// }
				// catch (InterruptedException e)
				// {
				// }
				// // log("Worker#" + this.getId() + " wakeup");
				// }
				//
				// if (this.isStopping())
				// {
				// break;
				// }
				// }

				// synchronized (this)
				// {
				// this.setRunning(true);
				// }

				// log("Worker#" + id + " wakeup");
				this.getMaster().reportReading(this);

				badReads = this.readRecords(this.getMaster().getParser(), this.getRecords());

				this.getMaster().reportRead(this);

				int[] result = this.doBatch(this.getConnection(), this.getStatement(), this.getTemplate().getIndexes(),
						this.getRecords());

				this.getMaster().reportLoaded(this, result[0] + badReads, result[1] + badReads);
			}
		}
		catch (SQLException ex)
		{
			ex.printStackTrace();
		}
		finally
		{
			this.destroy();
			// synchronized (this)
			// {
			// this.notifyAll();
			// }
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

	protected LoadWorker setNeedWait(boolean needWait)
	{
		this.needWait = needWait;
		return this;
	}

	protected LoadWorker setRecords(LinkedList<Record> records)
	{
		this.records = records;
		return this;
	}

	protected LoadWorker setStarted(boolean waiting)
	{
		this.started = waiting;
		return this;
	}

	// protected LoadWorker setRunning(boolean running)
	// {
	// this.running = running;
	// return this;
	// }

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

	// protected LoadWorker setWaiting(boolean waiting)
	// {
	// this.waiting = waiting;
	// return this;
	// }

	public Thread start()
	{
		Thread t = null;

		lock.lock();
		try
		{
			t = this.getThread();
			if (t != null)
			{
				return t;
			}
			t = new Thread(this);
			this.setThread(t);
		}
		finally
		{
			lock.unlock();
		}

		t.start();

		this.waitForStarted();

		return t;
	}

	public void stop()
	{
		lock.lock();
		try
		{
			if (this.getThread() != null)
			{
				this.setStopping(true);
				untilWakeup.signalAll();
				// t.interrupt();
				// while (!this.isWaiting())
				// {
				// try
				// {
				// untilWaiting.await();
				// }
				// catch (InterruptedException e)
				// {
				// }
				// }
				// untilWakeup.signalAll();
			}
		}
		finally
		{
			lock.unlock();
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

	public LoadWorker waitForStarted()
	{
		lock.lock();
		try
		{
			while (!this.isStarted())
			{
				try
				{
					notStarted.await();
				}
				catch (InterruptedException e)
				{
				}
			}
		}
		finally
		{
			lock.unlock();
		}
		return this;
	}

	public void wakeup()
	{
		lock.lock();
		try
		{
			if (this.getThread() != null)
			{
				// while (!this.isStopping() && !this.isWaiting())
				// {
				// try
				// {
				// untilWaiting.await();
				// }
				// catch (InterruptedException e)
				// {
				// }
				// }

				this.setNeedWait(false);
				untilWakeup.signalAll();
			}
		}
		finally
		{
			lock.unlock();
		}
	}
}
