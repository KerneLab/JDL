package org.kernelab.jdl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.kernelab.basis.sql.DataBase;

public class LoadMaster implements Runnable
{
	private RecordParser				parser;

	private InsertTemplate				template;

	private DataBase					dataBase;

	private long[]						result			= new long[] { 0L, 0L };

	private Set<LoadWorker>				workers			= new LinkedHashSet<LoadWorker>();

	private LinkedList<LoadWorker>		readyWorkers	= new LinkedList<LoadWorker>();

	private LinkedBlockingQueue<Record>	recordsQueue	= new LinkedBlockingQueue<Record>(10000);

	public DataBase getDataBase()
	{
		return dataBase;
	}

	public RecordParser getParser()
	{
		return parser;
	}

	public LinkedBlockingQueue<Record> getRecordsQueue()
	{
		return recordsQueue;
	}

	public InsertTemplate getTemplate()
	{
		return template;
	}

	protected Connection newConnection() throws ClassNotFoundException, SQLException
	{
		return this.getDataBase().newConnection();
	}

	protected LoadWorker newWorker() throws Exception
	{
		return new LoadWorker(this, newConnection());
	}

	protected void reportDestroy(LoadWorker worker)
	{
		synchronized (this.workers)
		{
			this.workers.remove(worker);
		}
	}

	protected void reportLoaded(LoadWorker worker, int total, int bads)
	{
		synchronized (this.result)
		{
			this.result[0] += total;
			this.result[1] += bads;
		}

		synchronized (this.readyWorkers)
		{
			this.readyWorkers.add(worker);
		}

		synchronized (this)
		{
			this.notifyAll();
		}
	}

	protected void reportRead(LoadWorker worker)
	{
		synchronized (this)
		{
			this.notifyAll();
		}
	}

	@Override
	public void run()
	{
		new Thread(new Runnable()
		{
			@Override
			public void run()
			{
				while (parser.hasNext())
				{
					try
					{
						recordsQueue.put(parser.next());
					}
					catch (InterruptedException e)
					{
					}
				}
				try
				{
					recordsQueue.put(RecordParser.END);
				}
				catch (InterruptedException e)
				{
				}
				LoadMaster.this.stop();
			}
		}).start();

		try
		{
			synchronized (this.workers)
			{
				LoadWorker newWorker = this.newWorker();
				this.workers.add(newWorker);
				newWorker.start();
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
			return;
		}

		while (true)
		{
			synchronized (this)
			{
				try
				{
					this.wait();
				}
				catch (InterruptedException e)
				{
				}
			}

			if (!this.parser.hasNext())
			{
				break;
			}
			else
			{
				shiftWorker();
			}
		}
	}

	public LoadMaster setDataBase(DataBase dataBase)
	{
		this.dataBase = dataBase;
		return this;
	}

	public LoadMaster setParser(RecordParser parser)
	{
		this.parser = parser;
		return this;
	}

	protected LoadMaster setRecordsQueue(LinkedBlockingQueue<Record> recordsQueue)
	{
		this.recordsQueue = recordsQueue;
		return this;
	}

	public LoadMaster setTemplate(InsertTemplate template)
	{
		this.template = template;
		return this;
	}

	protected void shiftWorker()
	{
		synchronized (this.readyWorkers)
		{
			if (!this.readyWorkers.isEmpty())
			{
				this.readyWorkers.removeFirst().wakeup();
				return;
			}
		}

		synchronized (this.workers)
		{
			while (this.workers.size() < 4)
			{
				try
				{
					LoadWorker newWorker = this.newWorker();
					this.workers.add(newWorker);
					newWorker.start();
					return;
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}

	public void stop()
	{
		synchronized (this.workers)
		{
			for (LoadWorker worker : this.workers)
			{
				worker.stop();
			}
		}
	}
}
