package org.kernelab.jdl;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
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
import org.kernelab.basis.sql.SQLKit;

public class CommandClient
{
	public static CommandClient newInstance(String[] args)
	{
		Entrance entr = new Entrance().handle(args);
		return new CommandClient() //
				.setDataBase(entr.parameter("url"), entr.parameter("usr"), entr.parameter("pwd")) //
				.setHint(entr.parameter("hint")) //
		;
	}

	public static OutputStreamWriter outputStreamToWriter(OutputStream os, String charSet)
	{
		try
		{
			return new OutputStreamWriter(System.out, "UTF-8");
		}
		catch (UnsupportedEncodingException e)
		{
			return new OutputStreamWriter(System.out);
		}
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

	private PrintWriter			out		= new PrintWriter(outputStreamToWriter(System.out, "UTF-8"), true);

	private PrintWriter			err		= new PrintWriter(outputStreamToWriter(System.err, "UTF-8"), true);

	private String				url;

	private String				username;

	private String				password;

	private ConnectionFactory	dataBase;

	private Connection			connection;

	private String				hint	= "SQL>";

	private boolean				exit	= false;

	protected void ensureConnection() throws SQLException
	{
		if (this.getConnection() == null)
		{
			this.setConnection(this.newConnection());
		}
	}

	public boolean execute(String cmd)
	{
		try
		{
			if (cmd.trim().equalsIgnoreCase("exit"))
			{
				this.exit = true;
			}
			else if (cmd.matches("(?is)^\\s*LOAD\\s+DATA\\s+.+$"))
			{
				LoadBuilder build = new LoadBuilder().resolve(cmd);

				LoadMaster master = new LoadMaster().setDataBase(this.getDataBase()).setParser(build.buildParser())
						.setTemplate(build.buildTemplate()).setOut(this.getOut()).setErr(this.getErr());

				long ts = System.currentTimeMillis();

				new Thread(master).start();

				master.waitForStopped();

				this.getOut().println("Elapsed: " + ((System.currentTimeMillis() - ts) / 1000) + "s");
				this.getOut().println("Total: " + master.getResult()[0]);
				this.getOut().println("Error: " + master.getResult()[1]);

				if (master.getResult()[1] > 0)
				{
					return false;
				}
			}
			else
			{
				Object res = executeSQL(cmd);
				output(res, 0, "  ");
				this.getOut().println();
			}

			return true;
		}
		catch (Exception e)
		{
			e.printStackTrace(this.getErr());
			return false;
		}
	}

	protected Object executeSQL(String sql) throws SQLException
	{
		Statement stmt = null;
		try
		{
			this.ensureConnection();

			stmt = this.getConnection().createStatement();

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
					e.printStackTrace();
				}
			}
		}
	}

	protected Connection getConnection()
	{
		return connection;
	}

	protected ConnectionFactory getDataBase()
	{
		return dataBase;
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

	public String getPassword()
	{
		return password;
	}

	public String getUrl()
	{
		return url;
	}

	public String getUsername()
	{
		return username;
	}

	public boolean interact() throws IOException
	{
		List<String> lines = new LinkedList<String>();

		Scanner input = null;
		try
		{
			input = new Scanner(new InputStreamReader(System.in, "UTF-8"));
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

				lines.add(line);

				if (line.matches("^.*\\s*;\\s*$"))
				{
					try
					{
						String cmd = Tools.jointStrings("\n", lines).replaceFirst("(?is)^(.+)\\s*?;\\s*$", "$1");
						if (!execute(cmd))
						{
							return false;
						}
					}
					finally
					{
						lines.clear();
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
		}

		return true;
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

	protected boolean isExit()
	{
		return exit;
	}

	protected Connection newConnection() throws SQLException
	{
		return this.getDataBase().newConnection();
	}

	public void output(Object msg, boolean wrapLine)
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

	protected CommandClient setConnection(Connection connection)
	{
		this.connection = connection;
		return this;
	}

	protected CommandClient setDataBase(ConnectionFactory db)
	{
		this.dataBase = db;
		return this;
	}

	public CommandClient setDataBase(String url, String username, String password)
	{
		return this.setUrl(url).setUsername(username).setPassword(password)
				.setDataBase(new ConnectionFactory(url, username, password));
	}

	protected CommandClient setErr(PrintWriter err)
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
		this.hint = hint != null ? hint : "SQL>";
		return this;
	}

	protected CommandClient setOut(PrintWriter output)
	{
		this.out = output;
		return this;
	}

	protected CommandClient setPassword(String password)
	{
		this.password = password;
		return this;
	}

	protected CommandClient setUrl(String url)
	{
		this.url = url;
		return this;
	}

	protected CommandClient setUsername(String username)
	{
		this.username = username;
		return this;
	}
}
