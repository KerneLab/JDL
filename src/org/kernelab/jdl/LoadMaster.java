package org.kernelab.jdl;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;

import org.kernelab.basis.sql.DataBase;

public class LoadMaster
{
	private RecordParser			parser;

	private InsertTemplate			template;

	private DataBase				dataBase;

	private long[]					result			= new long[] { 0L, 0L };

	private Set<LoadWorker>			workers			= new LinkedHashSet<LoadWorker>();

	private LinkedList<LoadWorker>	readyWorkers	= new LinkedList<LoadWorker>();

	public DataBase getDataBase()
	{
		return dataBase;
	}

	public RecordParser getParser()
	{
		return parser;
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
	}

	protected void reportRead(LoadWorker worker)
	{
		if (!this.parser.hasNext())
		{
			worker.stop();
			synchronized (this.workers)
			{
				for (LoadWorker w : this.workers)
				{
					if (worker != w)
					{
						w.stop();
					}
				}
			}
			return;
		}

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

	public LoadMaster setTemplate(InsertTemplate template)
	{
		this.template = template;
		return this;
	}

	public LoadMaster start() throws Exception
	{
		newWorker().start();
		return this;
	}
}
