package org.kernelab.jdl;

import java.io.File;
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
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kernelab.basis.Canal;
import org.kernelab.basis.Entrance;
import org.kernelab.basis.JSON;
import org.kernelab.basis.JSON.JSAN;
import org.kernelab.basis.Mapper;
import org.kernelab.basis.Tools;
import org.kernelab.basis.Variable;
import org.kernelab.basis.sql.DataBase;
import org.kernelab.basis.sql.DataBase.GeneralDataBase;
import org.kernelab.basis.sql.SQLKit;

public class CommandClient
{
	public static final String		KEY_DICT				= "dict";

	public static final String		KEY_LINK				= "link";

	public static final String		KEY_HINT				= "hint";

	public static final String		KEY_URL					= "url";

	public static final String		KEY_USR					= "usr";

	public static final String		KEY_PWD					= "pwd";

	public static final String		KEY_CRYPT				= "crypt";

	public static final String		KEY_QUIET				= "quiet";

	public static final String		KEY_CONC				= "conc";

	public static final String		KEY_REBALANCE			= "rebalance";

	public static final String		KEY_BATCH_SIZE			= "batchSize";

	public static final String		KEY_REWRITE_BATCH		= "rewriteBatch";

	public static final String		KEY_AUTO_COMMIT			= "autoCommit";

	public static final String		KEY_IGNORE_ERROR		= "ignoreError";

	public static final String		KEY_USE_RAW_CMD			= "useRawCmd";

	public static final int			REBALANCE_NONE			= 0;

	public static final int			REBALANCE_PICKONE		= 1;

	public static final int			REBALANCE_REARRANGE		= 2;

	public static final String		DEFAULT_HINT			= "SQL>";

	public static final String		DEFAULT_DELIMITER		= ";";

	public static final int			DEFAULT_REBALANCE		= REBALANCE_NONE;

	public static final int			DEFAULT_BATCH_SIZE		= 500;

	public static final boolean		DEFAULT_REWRITE_BATCH	= false;

	protected static final Pattern	REGEX_LINK				= Pattern.compile("^LINK\\b(.*)$",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	protected static final Pattern	REGEX_ECHO				= Pattern.compile("^ECHO\\b(.*)$",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	protected static final Pattern	REGEX_QUIET				= Pattern.compile("^QUIET\\b(.*)$",
			Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	protected static final Pattern	REGEX_DELIMITER			= Pattern.compile("^DELIMITER\\b(.*)$",
			Pattern.CASE_INSENSITIVE);

	protected static Set<Class<?>>	BASICS					= new HashSet<Class<?>>();

	static
	{
		BASICS.addAll(Tools.getBoxingTypes());
		BASICS.addAll(Tools.getPrimitiveTypes());
		BASICS.add(String.class);
		BASICS = Collections.unmodifiableSet(BASICS);
	}

	public static String composeErrorText(String cmd, Throwable err)
	{
		return Tools.textOf(new StringBuilder("\nErrorCatch: " + (cmd != null ? "[" + cmd + "] " : "")), err, 20)
				.toString();
	}

	public static String decode(String text)
	{
		return text;
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

	public static CommandClient newInstance(String[] args) throws Exception
	{
		Entrance entr = new Entrance().handle(args);
		return new CommandClient() //
				.setEntr(entr) //
				.setHint(entr.parameter(KEY_HINT)) //
				.setDict(entr.parameter(KEY_DICT)) //
				.setQuiet("true".equalsIgnoreCase(entr.parameter(KEY_QUIET))
						|| (entr.hasParameter(KEY_QUIET) && Tools.isNullOrWhite(entr.parameter(KEY_QUIET)))) //
				.setIgnoreError("true".equalsIgnoreCase(entr.parameter(KEY_IGNORE_ERROR))) //
				.link(entr.parameter(KEY_LINK));
	}

	public static PrintWriter printerOf(OutputStream os)
	{
		return new PrintWriter(writerOf(os), true);
	}

	public static void printError(PrintWriter out, Throwable err)
	{
		printError(out, err, null);
	}

	public static void printError(PrintWriter out, Throwable err, String cmd)
	{
		if (out == null || err == null)
		{
			return;
		}
		out.println(composeErrorText(cmd, err));
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
		}).transforms(Clob.class, new JSON.Transform<String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String transform(JSON json, String entry, Object value)
			{
				Reader r = null;
				try
				{
					r = ((Clob) value).getCharacterStream();
					return Tools.readerToStringBuilder(r).toString();
				}
				catch (Exception e)
				{
					throw Tools.getRuntimeException(e);
				}
				finally
				{
					if (r != null)
					{
						try
						{
							r.close();
						}
						catch (IOException e)
						{
						}
					}
				}
			}
		}).transforms(RowId.class, new JSON.Transform<String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String transform(JSON json, String entry, Object value)
			{
				byte[] bs = ((RowId) value).getBytes();
				StringBuilder buf = new StringBuilder(bs.length);
				for (byte b : bs)
				{
					buf.append((char) b);
				}
				return buf.toString();
			}
		}).transforms(Blob.class, new JSON.Transform<String>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public String transform(JSON json, String entry, Object value)
			{
				InputStream is = null;
				try
				{
					is = ((Blob) value).getBinaryStream();

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
					throw Tools.getRuntimeException(e);
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
				return Tools.jointStrings(" ", Tools.dumpBytes((byte[]) value));
			}
		}).transforms(Object.class, new JSON.Transform<Object>()
		{
			private static final long serialVersionUID = 1L;

			@Override
			public Object transform(JSON json, String entry, Object value)
			{
				if (BASICS.contains(value.getClass()))
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

	private PrintWriter							out				= new PrintWriter(writerOf(System.out), true);

	private PrintWriter							err				= new PrintWriter(writerOf(System.err), true);

	private String								delimiter		= DEFAULT_DELIMITER;

	private String								hint			= DEFAULT_HINT;

	private String								link;

	private Entrance							entr;

	private Map<String, Map<String, String>>	dict			= new LinkedHashMap<String, Map<String, String>>();

	private boolean								quiet			= false;

	private int									conc			= 1;

	private int									rebalance		= DEFAULT_REBALANCE;

	private boolean								autoCommit		= true;

	private boolean								ignoreError		= false;

	private boolean								useRawCmd		= false;

	private int									batchSize		= DEFAULT_BATCH_SIZE;

	private boolean								rewriteBatch	= DEFAULT_REWRITE_BATCH;

	private DataBase							dataBase;

	private Connection							connection;

	private Statement							statement;

	private LoadMaster							loader;

	private boolean								success			= true;

	private boolean								exit			= false;

	protected void closeConnection() throws SQLException
	{
		Connection conn = this.getConnection(false);
		if (conn != null)
		{
			try
			{
				try
				{
					if (!this.isAutoCommit())
					{
						if (this.isSuccess())
						{
							conn.commit();
						}
						else
						{
							conn.rollback();
						}
					}
				}
				catch (SQLException e)
				{
					if (this.isIgnoreError())
					{
						printError(e);
					}
					else
					{
						throw e;
					}
				}

				try
				{
					conn.close();
				}
				catch (SQLException e)
				{
					if (this.isIgnoreError())
					{
						printError(e);
					}
					else
					{
						throw e;
					}
				}
			}
			finally
			{
				this.statement = null;
				this.connection = null;
			}
		}
	}

	protected String decrypt(String cls, String pwd, Map<String, String> prop) throws Exception
	{
		return cls != null ? ((Cryptor) Class.forName(cls).newInstance()).decrypt(pwd, prop) : pwd;
	}

	public void echo(String msg)
	{
		output(msg != null ? msg.trim() : "null", true);
	}

	public boolean execute(String cmd)
	{
		try
		{
			cmd = cmd.trim();
			Matcher m = null;
			if (Tools.isNullOrWhite(cmd))
			{
			}
			else if (cmd.equalsIgnoreCase("EXIT"))
			{
				this.closeConnection();
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
			else if ((m = REGEX_LINK.matcher(cmd)).matches())
			{
				this.link(m.group(1));
			}
			else if ((m = REGEX_ECHO.matcher(cmd)).matches())
			{
				this.echo(m.group(1));
			}
			else if ((m = REGEX_QUIET.matcher(cmd)).matches())
			{
				this.setQuiet(m.group(1));
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
			this.printError(e, cmd);
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

	protected Map<String, Map<String, String>> getDict()
	{
		return dict;
	}

	protected Entrance getEntr()
	{
		return entr;
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

	protected String getLink()
	{
		return link;
	}

	protected PrintWriter getOut()
	{
		return out;
	}

	public String getParam(String name, Map<String, String> prop)
	{
		return getParam(name, prop, null);
	}

	protected String getParam(String name, Map<String, String> prop, String deft)
	{
		String value = prop != null ? prop.get(name) : null;
		if (value == null)
		{
			value = this.getEntr().parameter(name);
		}
		return value != null ? value : deft;
	}

	public int getRebalance()
	{
		return rebalance;
	}

	public boolean interact()
	{
		List<String> lines = new LinkedList<String>();
		Scanner input = null;
		Matcher m = null;
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

				if ((m = REGEX_DELIMITER.matcher(line)).matches())
				{
					this.setDelimiter(m.group(1));
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
							this.setSuccess(execute(cmd));
							if (!this.isSuccess() && !this.isIgnoreError())
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
			try
			{
				this.closeConnection();
			}
			catch (SQLException e)
			{
				printError(e);
				this.setSuccess(false);
			}
		}

		return this.isSuccess();
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

	public boolean isQuiet()
	{
		return quiet;
	}

	public boolean isRewriteBatch()
	{
		return rewriteBatch;
	}

	public boolean isSuccess()
	{
		return success;
	}

	public boolean isUseRawCmd()
	{
		return useRawCmd;
	}

	protected CommandClient link(Map<String, String> prop) throws Exception
	{
		String url = getParam(KEY_URL, prop);
		String usr = getParam(KEY_USR, prop);
		String pwd = getParam(KEY_PWD, prop);

		String crypt = getParam(KEY_CRYPT, prop);
		Map<String, String> map = new HashMap<String, String>();
		map.put(KEY_URL, url);
		map.put(KEY_USR, usr);
		map.put(KEY_PWD, pwd);
		if (prop != null && prop.containsKey(KEY_LINK))
		{
			map.put(KEY_LINK, prop.get(KEY_LINK));
		}
		pwd = decrypt(crypt, pwd, map);

		return this.setDataBase(url, usr, pwd) //
				.setConcurrency(Variable.asInteger(getParam(KEY_CONC, prop), 1)) //
				.setRebalance(Variable.asInteger(getParam(KEY_REBALANCE, prop), DEFAULT_REBALANCE)) //
				.setBatchSize(Variable.asInteger(getParam(KEY_BATCH_SIZE, prop), DEFAULT_BATCH_SIZE)) //
				.setRewriteBatch("true".equalsIgnoreCase(getParam(KEY_REWRITE_BATCH, prop))) //
				.setAutoCommit(!"false".equalsIgnoreCase(getParam(KEY_AUTO_COMMIT, prop))) //
				.setIgnoreError("true".equalsIgnoreCase(getParam(KEY_IGNORE_ERROR, prop))) //
				.setUseRawCmd("true".equalsIgnoreCase(getParam(KEY_USE_RAW_CMD, prop))) //
		;
	}

	public CommandClient link(String link) throws Exception
	{
		link = Tools.notNullOrWhite(link) ? link.trim() : null;
		if (this.getLink() == null || (link != null && !Tools.equals(this.getLink(), link)))
		{
			this.closeConnection();
			Map<String, String> prop = this.getDict().get(link);
			if (prop == null && link != null)
			{
				throw new NoSuchElementException(link);
			}
			this.link(prop);
			this.setLink(link);
		}

		if (this.getLink() != null && (!this.isQuiet() || link == null))
		{
			output(this.getLink(), true);
		}

		return this;
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
		printError(err, null);
	}

	protected void printError(Throwable err, String cmd)
	{
		printError(this.getErr(), err, cmd);
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

	protected CommandClient setDict(Map<String, Map<String, String>> dict)
	{
		this.dict = dict;
		return this;
	}

	protected CommandClient setDict(String path)
	{
		this.getDict().clear();

		if (path == null)
		{
			return this;
		}

		File file = new File(path);
		if (file.isFile())
		{
			JSON json = JSON.Parse(file, "UTF-8");
			if (json != null)
			{
				for (String key : json.keySet())
				{
					JSON val = json.attrJSON(key);
					if (val != null)
					{
						Map<String, String> prop = Canal.of(val).mapValues(new Mapper<Object, String>()
						{
							@Override
							public String map(Object el) throws Exception
							{
								return el != null ? el.toString() : null;
							}
						}).collectAsMap();
						prop.put(KEY_LINK, key);
						this.getDict().put(key, prop);
					}
				}
			}
		}

		return this;
	}

	protected CommandClient setEntr(Entrance entr)
	{
		this.entr = entr;
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

	protected CommandClient setLink(String link)
	{
		this.link = link;
		return this;
	}

	public CommandClient setOut(PrintWriter output)
	{
		this.out = output;
		return this;
	}

	public CommandClient setQuiet(boolean silent)
	{
		this.quiet = silent;
		return this;
	}

	protected CommandClient setQuiet(String quiet)
	{
		return this.setQuiet(!"false".equalsIgnoreCase(quiet != null ? quiet.trim() : null));
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

	protected CommandClient setSuccess(boolean success)
	{
		this.success &= success;
		return this;
	}

	public CommandClient setUseRawCmd(boolean rawcmd)
	{
		this.useRawCmd = rawcmd;
		return this;
	}
}
