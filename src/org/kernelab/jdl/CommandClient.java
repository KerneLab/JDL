package org.kernelab.jdl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.kernelab.basis.Entrance;
import org.kernelab.basis.JSON;
import org.kernelab.basis.JSON.JSAN;
import org.kernelab.basis.Tools;
import org.kernelab.basis.Variable;
import org.kernelab.basis.sql.DataBase;
import org.kernelab.basis.sql.DataBase.GeneralDataBase;
import org.kernelab.basis.sql.SQLKit;

public class CommandClient
{
	public static final int			REBALANCE_NONE			= 0;

	public static final int			REBALANCE_PICKONE		= 1;

	public static final int			REBALANCE_REARRANGE		= 2;

	public static final String		DEFAULT_HINT			= "SQL>";

	public static final String		DEFAULT_DELIMITER		= ";";

	public static final int			DEFAULT_REBALANCE		= REBALANCE_NONE;

	public static final int			DEFAULT_BATCH_SIZE		= 500;

	public static final boolean		DEFAULT_REWRITE_BATCH	= false;

	protected static Set<Class<?>>	BASICS					= new HashSet<Class<?>>();

	static
	{
		BASICS.addAll(Tools.getBoxingTypes());
		BASICS.addAll(Tools.getPrimitiveTypes());
		BASICS.add(String.class);
		BASICS = Collections.unmodifiableSet(BASICS);
	}

	public static String composeErrorText(Throwable err)
	{
		StringBuilder buf = new StringBuilder(
				"\nErrorCatch: " + err.getClass().getName() + ": " + err.getLocalizedMessage());

		int i = 0;
		for (StackTraceElement trace : err.getStackTrace())
		{
			buf.append("\n\tat " + trace);
			i++;
			if (i >= 20)
			{
				break;
			}
		}

		return buf.toString();
	}

	public static void main(String[] args)
	{
		try
		{
			System.exit(newInstance(args).interact() ? 0 : 1);
		}
		catch (Throwable e)
		{
			printError(printerOf(System.err), e);
			System.exit(1);
		}
	}

	public static CommandClient newInstance(String[] args)
	{
		Entrance entr = new Entrance().handle(args);
		return new CommandClient() //
				.setDataBase(entr.parameter("url"), entr.parameter("usr"), entr.parameter("pwd")) //
				.setConcurrency(Variable.asInteger(entr.parameter("conc"), 0)) //
				.setRebalance(Variable.asInteger(entr.parameter("rebalance"), DEFAULT_REBALANCE)) //
				.setBatchSize(Variable.asInteger(entr.parameter("batchSize"), DEFAULT_BATCH_SIZE)) //
				.setRewriteBatch("true".equalsIgnoreCase(entr.parameter("rewriteBatch"))) //
				.setAutoCommit(!"false".equalsIgnoreCase(entr.parameter("autoCommit"))) //
				.setIgnoreError("true".equalsIgnoreCase(entr.parameter("ignoreError"))) //
				.setUseRawCmd("true".equalsIgnoreCase(entr.parameter("useRawCmd"))) //
				.setHint(entr.parameter("hint")) //
		;
	}

	public static PrintWriter printerOf(OutputStream os)
	{
		return new PrintWriter(writerOf(os), true);
	}

	public static void printError(PrintWriter out, Throwable err)
	{
		if (out == null || err == null)
		{
			return;
		}
		out.println(composeErrorText(err));
	}

	public static Reader readerOf(InputStream is)
	{
		return new InputStreamReader(is, Charset.defaultCharset());
	}

	public static final JSAN resultSetToJSAN(ResultSet rs) throws SQLException
	{
		JSAN jsan = new JSAN();

		jsan.transforms(Date.class, new JSON.Transform<String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String transform(JSON json, String entry, Object value)
			{
				return Tools.getDateTimeString((Date) value, Tools.LOCAL_DATETIME_FORMAT);
			}
		}).transforms(Blob.class, new JSON.Transform<String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String transform(JSON json, String entry, Object value)
			{
				Blob blob = (Blob) value;

				InputStream is = null;
				try
				{
					is = blob.getBinaryStream();

					Collection<String> hex = new LinkedList<String>();

					byte[] buf = new byte[1024];
					int read = -1;
					while ((read = is.read(buf)) != -1)
					{
						hex.add(Tools.jointStrings(" ", Tools.dumpBytes(buf, 16, 0, read)));
					}

					return Tools.jointStrings(" ", hex);
				}
				catch (Exception e)
				{
					return e.getLocalizedMessage();
				}
				finally
				{
					if (is != null)
					{
						try
						{
							is.close();
						}
						catch (Exception e)
						{
						}
					}
				}
			}
		}).transforms(byte[].class, new JSON.Transform<String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String transform(JSON json, String entry, Object value)
			{
				return Tools.jointStrings(" ", Tools.dumpBytes(((byte[]) value)));
			}
		}).transforms(Object.class, new JSON.Transform<Object>()
		{
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Object transform(JSON json, String entry, Object value)
			{
				if (value == null)
				{
					return null;
				}
				else if (BASICS.contains(value.getClass()))
				{
					return value;
				}
				else if (value instanceof JSON)
				{
					return value;
				}
				else
				{
					return value.toString();
				}
			}
		});

		ResultSetMetaData meta = rs.getMetaData();

		jsan.add(new JSAN().addAll(SQLKit.listNameOfMetaData(meta)));

		SQLKit.jsanOfResultSet(rs, jsan, SQLKit.mapIndexOfMetaData(meta), JSAN.class, -1);

		return jsan;
	}

	public static Writer writerOf(OutputStream os)
	{
		return new OutputStreamWriter(os, Charset.defaultCharset());
	}

	private PrintWriter	out				= new PrintWriter(writerOf(System.out), true);

	private PrintWriter	err				= new PrintWriter(writerOf(System.err), true);

	private String		delimiter		= DEFAULT_DELIMITER;

	private String		hint			= DEFAULT_HINT;

	private int			conc			= 1;

	private int			rebalance		= DEFAULT_REBALANCE;

	private boolean		autoCommit		= true;

	private boolean		ignoreError		= false;

	private boolean		useRawCmd		= false;

	private int			batchSize		= DEFAULT_BATCH_SIZE;

	private boolean		rewriteBatch	= DEFAULT_REWRITE_BATCH;

	private DataBase	dataBase;

	private Connection	connection;

	private Statement	statement;

	private LoadMaster	loader;

	private boolean		exit			= false;

	public boolean execute(String cmd)
	{
		try
		{
			cmd = cmd.trim();
			if (cmd.equalsIgnoreCase("EXIT"))
			{
				this.exit = true;
			}
			else if (cmd.equalsIgnoreCase("COMMIT"))
			{
				this.getConnection().commit();
			}
			else if (cmd.equalsIgnoreCase("ROLLBACK"))
			{
				this.getConnection().rollback();
			}
			else
			{
				if (!this.isUseRawCmd())
				{
					if (cmd.matches("(?is)^\\s*LOAD\\s+DATA\\s+LOCAL\\s+INFILE\\s+.+$"))
					{
						return this.executeLoadData(cmd);
					}
				}
				Object res = executeSQL(cmd);
				output(res, 0, "  ");
				this.getOut().println();
			}

			return true;
		}
		catch (Throwable e)
		{
			this.printError(e);
			return false;
		}
	}

	protected boolean executeLoadData(String cmd) throws Exception
	{
		LoadBuilder build = new LoadBuilder().resolve(cmd);

		LoadMaster master = new LoadMaster() //
				.setConcurrency(this.getConcurrency())//
				.setBatchSize(this.getBatchSize()) //
				.setDataBase(this.getDataBase()) //
				.setRebalance(this.getRebalance()) //
				.setRewriteBatch(this.isRewriteBatch()) //
				.setParser(build.buildParser()) //
				.setTemplate(build.buildTemplate()) //
				.setOut(this.getOut()) //
				.setErr(this.getErr());

		long ts = System.currentTimeMillis();

		synchronized (this)
		{
			new Thread(master).start();
			this.loader = master;
		}

		master.waitForStopped();

		synchronized (this)
		{
			this.loader = null;
		}

		if (master.getResult() == null)
		{
			return false;
		}

		this.getOut().println("Done");
		this.getOut().println("Elapsed: " + ((System.currentTimeMillis() - ts) / 1000) + "s");
		this.getOut().println("Total: " + master.getResult()[0] + ", Error: " + master.getResult()[1]);

		if (master.getResult()[1] > 0)
		{
			return false;
		}
		else
		{
			return true;
		}
	}

	protected Object executeSQL(String sql) throws SQLException
	{
		Connection conn = this.getConnection();

		try
		{
			synchronized (this)
			{
				statement = conn.createStatement();
			}

			if (statement.execute(sql))
			{
				return resultSetToJSAN(statement.getResultSet());
			}
			else
			{
				return statement.getUpdateCount();
			}
		}
		finally
		{
			synchronized (this)
			{
				if (statement != null)
				{
					try
					{
						statement.close();
					}
					catch (Exception e)
					{
						printError(e);
					}
					statement = null;
				}
			}
		}
	}

	public int getBatchSize()
	{
		return batchSize;
	}

	public int getConcurrency()
	{
		return conc;
	}

	protected Connection getConnection() throws SQLException
	{
		return getConnection(true);
	}

	protected Connection getConnection(boolean newIfNull) throws SQLException
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
							connection = null;
						}
					}
					if (newIfNull)
					{
						Connection conn = this.newConnection();
						if (conn.isValid(5))
						{
							conn.setAutoCommit(this.isAutoCommit());
							connection = conn;
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

	protected DataBase getDataBase()
	{
		return dataBase;
	}

	protected String getDelimiter()
	{
		return delimiter;
	}

	protected PrintWriter getErr()
	{
		return err;
	}

	public String getHint()
	{
		return hint;
	}

	protected String getHintNextLine()
	{
		if (Tools.isNullOrEmpty(this.getHint()))
		{
			return "";
		}
		else
		{
			return Tools.repeat(' ', this.getHint().length() - 1) + "|";
		}
	}

	protected PrintWriter getOut()
	{
		return out;
	}

	public int getRebalance()
	{
		return rebalance;
	}

	public boolean interact()
	{
		List<String> lines = new LinkedList<String>();
		boolean suc = true;
		Scanner input = null;
		try
		{
			this.listenShutdown();

			input = new Scanner(readerOf(System.in));
			do
			{
				if (Tools.notNullOrEmpty(this.getHint()))
				{
					if (lines.isEmpty())
					{
						output(this.getHint() + " ", false);
					}
					else
					{
						output(this.getHintNextLine() + " ", false);
					}
				}

				String line = null;

				try
				{
					line = input.nextLine();
				}
				catch (Exception e)
				{
					break;
				}

				if (line.matches("(?i)^DELIMITER\\b(.*)$"))
				{
					this.setDelimiter(line.replaceFirst("(?i)^DELIMITER\\b(.*)$", "$1"));
				}
				else
				{
					lines.add(line);
					if (line.matches("^.*\\s*\\Q" + this.getDelimiter() + "\\E\\s*$"))
					{
						try
						{
							String cmd = Tools.jointStrings("\n", lines)
									.replaceFirst("(?s)^(.+)\\s*?\\Q" + this.getDelimiter() + "\\E\\s*$", "$1");
							suc &= execute(cmd);
							if (!suc && !this.isIgnoreError())
							{
								return false;
							}
						}
						finally
						{
							lines.clear();
						}
					}
				}

				if (this.isExit())
				{
					break;
				}
			}
			while (true);
		}
		finally
		{
			if (input != null)
			{
				input.close();
			}
			Connection conn = null;
			try
			{
				conn = this.getConnection(false);
				if (conn != null && !this.isAutoCommit())
				{
					conn.rollback();
				}
			}
			catch (SQLException e)
			{
				printError(e);
			}
			if (conn != null)
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

		return suc;
	}

	protected boolean isAllPrimaryData(JSON json)
	{
		for (Object value : json.values())
		{
			if (value instanceof JSON)
			{
				return false;
			}
		}
		return true;
	}

	public boolean isAutoCommit()
	{
		return autoCommit;
	}

	protected boolean isExit()
	{
		return exit;
	}

	public boolean isIgnoreError()
	{
		return ignoreError;
	}

	public boolean isRewriteBatch()
	{
		return rewriteBatch;
	}

	public boolean isUseRawCmd()
	{
		return useRawCmd;
	}

	protected void listenShutdown()
	{
		Runtime.getRuntime().addShutdownHook(new Thread()
		{
			@Override
			public void run()
			{
				synchronized (CommandClient.this)
				{
					if (statement != null)
					{
						try
						{
							statement.cancel();
						}
						catch (Exception e)
						{
						}
					}

					if (loader != null)
					{
						loader.stop();
					}
				}
			}
		});
	}

	protected Connection newConnection() throws SQLException
	{
		return this.getDataBase().newConnection();
	}

	protected void output(Object msg, boolean wrapLine)
	{
		if (this.getOut() != null)
		{
			if (wrapLine)
			{
				this.getOut().println(msg);
			}
			else
			{
				this.getOut().print(msg);
				this.getOut().flush();
			}
		}
	}

	protected void output(Object object, int indents, String indent) throws IOException
	{
		JSON.Output(this.getOut(), object, indents, indent);
	}

	protected void printError(Throwable err)
	{
		printError(this.getErr(), err);
	}

	public CommandClient setAutoCommit(boolean autoCommit)
	{
		this.autoCommit = autoCommit;
		return this;
	}

	public CommandClient setBatchSize(int batchSize)
	{
		this.batchSize = batchSize;
		return this;
	}

	public CommandClient setConcurrency(int conc)
	{
		this.conc = conc;
		return this;
	}

	public CommandClient setDataBase(DataBase db)
	{
		this.dataBase = db;
		return this;
	}

	public CommandClient setDataBase(String url, String username, String password)
	{
		return this.setDataBase(new GeneralDataBase(url, username, password)
		{
			/**
			 * 
			 */
			private static final long serialVersionUID = 5568975809408157880L;

			@Override
			public String getURL()
			{
				switch (CommandClient.this.getRebalance())
				{
					case REBALANCE_PICKONE:
						return DataBase.randomPickone(this.url);

					case REBALANCE_REARRANGE:
						return DataBase.randomRearrange(this.url);

					default:
						return this.url;
				}
			}
		});
	}

	protected CommandClient setDelimiter(String delimiter)
	{
		this.delimiter = Tools.isNullOrWhite(delimiter) ? DEFAULT_DELIMITER : delimiter.trim();
		return this;
	}

	public CommandClient setErr(PrintWriter err)
	{
		this.err = err;
		return this;
	}

	protected CommandClient setExit(boolean exit)
	{
		this.exit = exit;
		return this;
	}

	public CommandClient setHint(String hint)
	{
		this.hint = hint != null ? hint : DEFAULT_HINT;
		return this;
	}

	public CommandClient setIgnoreError(boolean ignore)
	{
		this.ignoreError = ignore;
		return this;
	}

	public CommandClient setOut(PrintWriter output)
	{
		this.out = output;
		return this;
	}

	public CommandClient setRebalance(int rebalance)
	{
		this.rebalance = rebalance;
		return this;
	}

	public CommandClient setRewriteBatch(boolean rewriteBatch)
	{
		this.rewriteBatch = rewriteBatch;
		return this;
	}

	public CommandClient setUseRawCmd(boolean rawcmd)
	{
		this.useRawCmd = rawcmd;
		return this;
	}
}
