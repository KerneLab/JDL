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
	protected static final int[]	EMPTY_RESULT	= new int[] { 0, 0 };

	private static int				ID				= 0;

	private final int				id				= ID++;

	protected final Lock			lock			= new ReentrantLock();

	// protected final Condition untilWaiting = lock.newCondition();

	protected final Condition		notStarted		= lock.newCondition();

	protected final Condition		untilWakeup		= lock.newCondition();

	private LoadMaster				master;

	private Connection				connection;

	private PreparedStatement		statement;

	private String					rewriteInsert;

	private PreparedStatement		rewriteStatement;

	// private boolean waiting = false;

	private Thread					thread;

	private boolean					needWait		= true;

	private boolean					stopping		= false;

	private boolean					started			= false;

	// private boolean running = false;

	private LinkedList<Record>		records			= new LinkedList<Record>();

	public LoadWorker(LoadMaster master)
	{
		this.setMaster(master);
	}

	protected void addBatch(PreparedStatement ps, int[] index, String[] data) throws SQLException
	{
		for (int i = 0; i < index.length; i++)
		{
			ps.setString(i + 1, data[index[i]]);
		}
		ps.addBatch();
	}

	protected int addRewrite(PreparedStatement ps, int[] index, List<Record> records) throws SQLException
	{
		String[] data = null;
		int offset = 0, i = 0;
		for (Record record : records)
		{
			data = record.data;
			for (i = 0; i < index.length; i++)
			{
				ps.setString(offset + i + 1, data[index[i]]);
			}
			offset += index.length;
		}
		return ps.executeUpdate();
	}

	protected int addUpdate(PreparedStatement ps, int[] index, String[] data) throws SQLException
	{
		for (int i = 0; i < index.length; i++)
		{
			ps.setString(i + 1, data[index[i]]);
		}
		return ps.executeUpdate();
	}

	protected int[] checkResult(int[] counts, LinkedList<Record> records)
	{
		int i = 0;
		LinkedList<Record> good = new LinkedList<Record>(), bads = new LinkedList<Record>();
		Record record = null;
		while (!records.isEmpty())
		{
			record = records.removeFirst();
			if (counts == null || counts[i] == Statement.EXECUTE_FAILED)
			{
				bads.add(record);
			}
			else
			{
				good.add(record);
			}
			i++;
		}

		int[] goodRes = this.doBatch(this.getConnection(), this.getTemplate(), good);
		int[] badsRes = this.trackBads(this.getConnection(), this.getStatement(), this.getTemplate(), bads);

		return new int[] { goodRes[0] + badsRes[0], goodRes[1] + badsRes[1] };
	}

	protected void destroy()
	{
		// log("destroying");
		LoadMaster master = null;

		lock.lock();
		try
		{
			master = this.getMaster();
			if (master != null)
			{
				this.setStatement(null);
				this.rewriteStatement = null;
				try
				{
					Connection conn = this.getConnection(false);
					if (conn != null)
					{
						conn.close();
					}
				}
				catch (Exception e)
				{
				}
				finally
				{
					try
					{
						this.setConnection(null);
					}
					catch (SQLException e)
					{
					}
				}
				this.getRecords().clear();
				this.setThread(null);
				this.setMaster(null);
			}
		}
		finally
		{
			lock.unlock();
		}

		if (master != null)
		{
			master.reportDestroy(this);
		}
		// log("destroyed");
	}

	protected int[] doBatch(Connection conn, InsertTemplate template, LinkedList<Record> records)
	{
		if (records.isEmpty())
		{
			return EMPTY_RESULT;
		}

		if (this.getMaster().isRewriteBatch())
		{
			return this.doBatchRewrite(conn, template, records);
		}
		else
		{
			return this.doBatchNormal(conn, this.getStatement(), template, records);
		}
	}

	protected int[] doBatchNormal(Connection conn, PreparedStatement ps, InsertTemplate template,
			LinkedList<Record> records)
	{
		int total = records.size();
		try
		{
			conn.setAutoCommit(false);

			ps.clearBatch();

			int[] index = template.getIndexes();

			for (Record record : records)
			{
				this.addBatch(ps, index, record.data);
			}

			ps.executeBatch();

			conn.commit();

			return new int[] { total, 0 };
		}
		catch (BatchUpdateException e)
		{
			int[] counts = e.getUpdateCounts();
			try
			{
				conn.rollback();
			}
			catch (SQLException ex)
			{
				printError(ex);
			}
			return this.checkResult(counts, records);
		}
		catch (SQLException e)
		{
			printError(e);
			try
			{
				conn.rollback();
			}
			catch (SQLException ex)
			{
				printError(ex);
			}
			return this.checkResult(null, records);
		}
		catch (Throwable e)
		{
			printError(e);
			return new int[] { total, total };
		}
	}

	protected int[] doBatchRewrite(Connection conn, InsertTemplate template, LinkedList<Record> records)
	{
		int total = records.size();
		PreparedStatement ps = null;
		try
		{
			conn.setAutoCommit(true);

			ps = this.getRewriteStatement(conn, template, records.size());

			this.addRewrite(ps, template.getIndexes(), records);

			return new int[] { total, 0 };
		}
		catch (SQLException e)
		{
			return this.checkResult(null, records);
		}
		catch (Throwable e)
		{
			printError(e);
			return new int[] { total, total };
		}
		finally
		{
			if (ps != null && ps != this.rewriteStatement)
			{
				try
				{
					ps.close();
				}
				catch (SQLException e)
				{
				}
			}
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

	public int getBatchSize()
	{
		return this.getMaster().getBatchSize();
	}

	protected Connection getConnection()
	{
		return getConnection(true);
	}

	protected Connection getConnection(boolean newIfNull)
	{
		do
		{
			try
			{
				if (connection == null || !connection.isValid(5))
				{
					if (connection != null)
					{
						try
						{
							connection.rollback();
						}
						catch (SQLException e)
						{
						}
						try
						{
							connection.close();
						}
						catch (SQLException e)
						{
						}
						finally
						{
							this.setConnection(null);
						}
					}
					if (newIfNull)
					{
						Connection conn = this.getMaster().newConnection();
						if (conn.isValid(5))
						{
							this.setConnection(conn);
						}
						else
						{
							try
							{
								conn.close();
							}
							catch (SQLException e)
							{
							}
						}
					}
				}
			}
			catch (SQLException e)
			{
				printError(e);
				try
				{
					Thread.sleep(10 * 1000);
				}
				catch (InterruptedException e1)
				{
				}
			}
		}
		while (connection == null && newIfNull);

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

	protected PreparedStatement getRewriteStatement(Connection conn, InsertTemplate template, int rows)
			throws SQLException
	{
		if (rows == this.getBatchSize())
		{
			if (this.rewriteStatement == null)
			{
				if (this.rewriteInsert == null)
				{
					this.rewriteInsert = template.getInsert(rows);
				}
				this.rewriteStatement = conn.prepareStatement(this.rewriteInsert);
			}
			return this.rewriteStatement;
		}
		else
		{
			return conn.prepareStatement(template.getInsert(rows));
		}
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

	protected void init(Connection conn) throws SQLException
	{
		conn.setAutoCommit(false);
		this.setStatement(conn.prepareStatement(this.getTemplate().getInsert()));
		this.rewriteStatement = null;
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

	protected void log(String msg)
	{
		Tools.debug(Tools.getDateTimeString() + " Worker#" + id + " " + msg);
	}

	protected void logBad(Record record, Exception ex)
	{
		record.printError(this.getMaster().getErr(), ex);
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

	protected void printError(Throwable err)
	{
		this.getMaster().printError(err);
	}

	protected int readRecords(RecordParser parser, List<Record> records)
	{
		int bads = 0;
		records.clear();
		Record record = null;
		int needCols = this.getTemplate().getItems().length;
		int batchSize = this.getBatchSize();
		for (int i = 0; i < batchSize; i++)
		{
			if (parser.hasNext())
			{
				record = parser.next();
				if (needCols <= record.data.length)
				{
					records.add(record);
				}
				else
				{
					bads++;
					this.logBad(record,
							new SQLException(record.data.length + " columns found but need " + needCols + " to load"));
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
				// // log("waiting");
				// try
				// {
				// this.wait();
				// }
				// catch (InterruptedException e)
				// {
				// }
				// // log("wakeup");
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

				// log("wakeup");
				this.getMaster().reportReading(this);

				badReads = this.readRecords(this.getMaster().getParser(), this.getRecords());

				this.getMaster().reportRead(this);

				// log("batch begin");
				int[] result = this.doBatch(this.getConnection(), this.getTemplate(), this.getRecords());
				// log("batch end");

				this.getMaster().reportLoaded(this, result[0] + badReads, result[1] + badReads);
			}
		}
		catch (Throwable ex)
		{
			printError(ex);
			this.getMaster().reportError(this);
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

	protected LoadWorker setConnection(Connection connection) throws SQLException
	{
		this.connection = connection;
		if (connection != null)
		{
			this.init(connection);
		}
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

	protected int[] trackBads(Connection conn, PreparedStatement ps, InsertTemplate template, LinkedList<Record> bads)
	{
		int[] index = template.getIndexes();
		int total = bads.size(), good = 0;
		try
		{
			conn.setAutoCommit(true);

			Record rec = null;
			while (!bads.isEmpty())
			{
				rec = bads.removeFirst();
				try
				{
					this.addUpdate(ps, index, rec.data);
					good++;
				}
				catch (SQLException err)
				{
					this.logBad(rec, err);
				}
			}
			return new int[] { total, total - good };
		}
		catch (SQLException e)
		{
			printError(e);
			return new int[] { total, total - good };
		}
		catch (Throwable e)
		{
			printError(e);
			return new int[] { total, total - good };
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
