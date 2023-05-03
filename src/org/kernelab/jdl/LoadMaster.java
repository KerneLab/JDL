package org.kernelab.jdl;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.kernelab.basis.JSON;
import org.kernelab.basis.Tools;

public class LoadMaster implements Runnable
{
	private RecordParser			parser;

	private InsertTemplate			template;

	private ConnectionFactory		dataBase;

	private long[]					result				= new long[] { 0L, 0L };

	protected final Lock			lock				= new ReentrantLock();

	protected final Condition		notReading			= lock.newCondition();

	protected final Condition		untilEmptyWorkers	= lock.newCondition();

	protected final Condition		notEmptyWorkers		= lock.newCondition();

	protected final Condition		untilStopped		= lock.newCondition();

	private boolean					reading				= false;

	private boolean					ended				= false;

	private boolean					stopped				= false;

	private Set<LoadWorker>			workers				= new LinkedHashSet<LoadWorker>();

	private LinkedList<LoadWorker>	readyWorkers		= new LinkedList<LoadWorker>();

	private Map<String, Boolean>	workersReading		= new LinkedHashMap<String, Boolean>();

	private PrintWriter				out					= new PrintWriter(
			CommandClient.outputStreamToWriter(System.out, "UTF-8"), true);

	private PrintWriter				err					= new PrintWriter(
			CommandClient.outputStreamToWriter(System.err, "UTF-8"), true);

	public ConnectionFactory getDataBase()
	{
		return dataBase;
	}

	protected PrintWriter getErr()
	{
		return err;
	}

	protected PrintWriter getOut()
	{
		return out;
	}

	public RecordParser getParser()
	{
		return parser;
	}

	public long[] getResult()
	{
		return result;
	}

	public InsertTemplate getTemplate()
	{
		return template;
	}

	protected void initWorkers() throws Exception
	{
		lock.lock();
		try
		{
			while (this.workers.size() < 4)
			{
				LoadWorker worker = this.newWorker();
				workersReading.put(String.valueOf(worker.getId()), false);
				this.workers.add(worker);
				this.readyWorkers.add(worker);
				worker.start();
			}
		}
		finally
		{
			lock.unlock();
		}
	}

	public boolean isStopping()
	{
		lock.lock();
		try
		{
			return ended;
		}
		finally
		{
			lock.unlock();
		}
	}

	protected void log(String log)
	{
		Tools.debug(log);
	}

	protected Connection newConnection() throws SQLException
	{
		return this.getDataBase().newConnection();
	}

	protected LoadWorker newWorker() throws Exception
	{
		return new LoadWorker(this, newConnection());
	}

	protected void reportDestroy(LoadWorker worker)
	{
		log("Worker#" + worker.getId() + " destroy");

		lock.lock();
		try
		{
			this.workers.remove(worker);
			this.untilEmptyWorkers.signalAll();
		}
		finally
		{
			lock.unlock();
		}
	}

	protected void reportLoaded(LoadWorker worker, int total, int bads)
	{
		// log("Worker#" + worker.getId() + " reporting loaded");

		lock.lock();
		try
		{
			this.result[0] += total;
			this.result[1] += bads;
			this.readyWorkers.add(worker);
			this.notEmptyWorkers.signalAll();
			// log("Worker#" + worker.getId() + " reported loaded");
		}
		finally
		{
			lock.unlock();
		}
	}

	protected void reportRead(LoadWorker worker)
	{
		// log("Worker#" + worker.getId() + " reporting read " + this.reading);

		lock.lock();
		try
		{
			workersReading.put(String.valueOf(worker.getId()), false);
			this.reading = false;
			if (!this.parser.hasNext())
			{
				this.ended = true;
			}
			notReading.signalAll(); // OK
			// log("Worker#" + worker.getId() + " reported read " +
			// this.reading);
		}
		finally
		{
			lock.unlock();
		}
	}

	protected void reportReading(LoadWorker worker)
	{
		// log("Worker#" + worker.getId() + " reporting reading");

		lock.lock();
		try
		{
			workersReading.put(String.valueOf(worker.getId()), true);
			// log("Worker#" + worker.getId() + " reported reading");
		}
		finally
		{
			lock.unlock();
		}
	}

	@Override
	public void run()
	{
		try
		{
			this.initWorkers();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return;
		}

		LoadWorker worker = null;
		lock.lock();
		try
		{
			this.stopped = false;
			this.reading = true;
			worker = this.shiftWorker();
		}
		finally
		{
			lock.unlock();
		}
		// log("Worker#" + worker.getId() + " shifting");
		worker.waitForStarted().wakeup();

		while (true)
		{
			worker = null;

			lock.lock();
			try
			{
				// log("master waiting " + this.reading);
				while (this.reading)
				{
					try
					{
						// block here
						if (!notReading.await(10, TimeUnit.SECONDS))
						{
							log("master wait timeout");
							String ids = "";
							for (LoadWorker w : this.readyWorkers)
							{
								ids += w.getId() + " ";
							}
							log("ready: " + ids);
							log("reading: " + new JSON().attrAll(this.workersReading).toString());
						}
					}
					catch (InterruptedException e)
					{
					}
				}
				// log("master wakeup " + this.reading);

				if (this.ended)
				{
					break;
				}

				this.reading = true;

				// log("master shifting " + this.reading);
				worker = this.shiftWorker();
				// log("master shifted " + this.reading + " " + worker.getId());
			}
			finally
			{
				lock.unlock();
			}

			// log("master waking up Worker#" + worker.getId());
			worker.waitForStarted().wakeup(); // OK
			// log("master waked up Worker#" + worker.getId());
		}

		log("Master stopping");
		this.stop();

		lock.lock();
		try
		{
			while (!this.workers.isEmpty())
			{
				try
				{
					this.untilEmptyWorkers.await();
				}
				catch (InterruptedException e)
				{
				}
			}

			this.stopped = true;
			this.untilStopped.signalAll();
		}
		finally
		{
			lock.unlock();
		}
	}

	public LoadMaster setDataBase(ConnectionFactory dataBase)
	{
		this.dataBase = dataBase;
		return this;
	}

	protected LoadMaster setErr(PrintWriter err)
	{
		this.err = err;
		return this;
	}

	protected LoadMaster setOut(PrintWriter out)
	{
		this.out = out;
		return this;
	}

	public LoadMaster setParser(RecordParser parser)
	{
		this.parser = parser;
		return this;
	}

	protected LoadMaster setResult(long[] result)
	{
		this.result = result;
		return this;
	}

	public LoadMaster setTemplate(InsertTemplate template)
	{
		this.template = template;
		return this;
	}

	protected LoadWorker shiftWorker()
	{
		while (this.readyWorkers.isEmpty())
		{
			try
			{
				this.notEmptyWorkers.await();
			}
			catch (InterruptedException e)
			{
			}
		}
		return this.readyWorkers.removeFirst();
	}

	public void stop()
	{
		Collection<LoadWorker> workers = new LinkedList<LoadWorker>();

		lock.lock();
		try
		{
			workers.addAll(this.workers);
		}
		finally
		{
			lock.unlock();
		}

		for (LoadWorker worker : workers)
		{
			// log("master stopping Worker#" + worker.getId());
			worker.stop();
			// log("master stoped Worker#" + worker.getId());
		}
	}

	public void waitForStopped()
	{
		lock.lock();
		try
		{
			while (!this.stopped)
			{
				try
				{
					this.untilStopped.await();
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
	}
}
