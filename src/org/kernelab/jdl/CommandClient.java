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
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.kernelab.basis.Entrance;
import org.kernelab.basis.JSON;
import org.kernelab.basis.JSON.JSAN;
import org.kernelab.basis.JSON.Pair;
import org.kernelab.basis.Tools;
import org.kernelab.basis.Variable;
import org.kernelab.basis.sql.DataBase;
import org.kernelab.basis.sql.SQLKit;

public class CommandClient
{
	public static final String	DEFAULT_HINT			= "SQL>";

	public static final String	DEFAULT_DELIMITER		= ";";

	public static final int		DEFAULT_REBALANCE		= ConnectionFactory.REBALANCE_NONE;

	public static final int		DEFAULT_BATCH_SIZE		= 500;

	public static final boolean	DEFAULT_REWRITE_BATCH	= false;

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
		});
		jsan.transforms(Blob.class, new JSON.Transform<String>()
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

	private PrintWriter			out				= new PrintWriter(writerOf(System.out), true);

	private PrintWriter			err				= new PrintWriter(writerOf(System.err), true);

	private String				delimiter		= DEFAULT_DELIMITER;

	private String				hint			= DEFAULT_HINT;

	private int					conc			= 1;

	private int					rebalance		= DEFAULT_REBALANCE;

	private boolean				autoCommit		= true;

	private boolean				ignoreError		= false;

	private boolean				useRawCmd		= false;

	private int					batchSize		= DEFAULT_BATCH_SIZE;

	private boolean				rewriteBatch	= DEFAULT_REWRITE_BATCH;

	private ConnectionFactory	dataBase;

	private Connection			connection;

	private boolean				exit			= false;

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
				.setRewriteBatch(this.isRewriteBatch()) //
				.setParser(build.buildParser()) //
				.setTemplate(build.buildTemplate()) //
				.setOut(this.getOut()).setErr(this.getErr());

		long ts = System.currentTimeMillis();

		new Thread(master).start();

		master.waitForStopped();

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

		Statement stmt = null;
		try
		{
			stmt = conn.createStatement();

			if (stmt.execute(sql))
			{
				return resultSetToJSAN(stmt.getResultSet());
			}
			else
			{
				return stmt.getUpdateCount();
			}
		}
		finally
		{
			if (stmt != null)
			{
				try
				{
					stmt.close();
				}
				catch (Exception e)
				{
					printError(e);
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
		if (connection == null || !connection.isValid(5))
		{
			if (connection != null)
			{
				try
				{
					connection.close();
				}
				catch (SQLException e)
				{
				}
				connection = null;
			}
			if (newIfNull)
			{
				connection = this.newConnection();
				connection.setAutoCommit(this.isAutoCommit());
			}
		}
		return connection;
	}

	protected ConnectionFactory getDataBase()
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
		PrintWriter out = this.getOut();

		if (out == null)
		{
			return;
		}

		if (!(object instanceof JSON))
		{
			out.print(Tools.repeat(indent, indents));
			out.print(JSON.SerializeValueOf(object));
			return;
		}

		JSON json = (JSON) object;
		if (json instanceof JSAN)
		{
			if (isAllPrimaryData(json))
			{
				out.print(Tools.repeat(indent, indents));
				JSON.Serialize(json, out, -1);
			}
			else
			{
				out.print(Tools.repeat(indent, indents));
				out.println("[");
				for (Object value : json)
				{
					output(value, indents + 1, indent);
					out.println(",");
				}
				out.print(Tools.repeat(indent, indents));
				out.print("]");
			}
		}
		else
		{
			out.print(Tools.repeat(indent, indents));
			out.println("{");
			for (Pair pair : json.pairs())
			{
				out.print(Tools.repeat(indent, indents + 1));
				out.print(JSON.SerializeValueOf(pair.getKey()));
				out.print(":");
				output(pair.getValue(), 0, "  ");
				out.println(",");
			}
			out.print(Tools.repeat(indent, indents));
			out.print("}");
		}
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

	public CommandClient setDataBase(ConnectionFactory db)
	{
		this.dataBase = db;
		return this;
	}

	public CommandClient setDataBase(String url, String username, String password)
	{
		return this.setDataBase(new ConnectionFactory(url, username, password)
		{
			@Override
			protected String getUrl()
			{
				switch (CommandClient.this.getRebalance())
				{
					case ConnectionFactory.REBALANCE_PICKONE:
						return DataBase.randomPickone(this.url);

					case ConnectionFactory.REBALANCE_REARRANGE:
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
