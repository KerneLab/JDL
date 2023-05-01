package org.kernelab.jdl;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.kernelab.basis.Tools;

public class RecordParser implements Iterator<Record>
{
	public static final Record END = new Record(0, null);

	protected static boolean samePrefix(char[] a, int from, char[] b)
	{
		for (int i = from; i < a.length; i++)
		{
			if (a[i] != b[i - from])
			{
				return false;
			}
		}
		return true;
	}

	protected static boolean startWith(char[] a, int from, char[] b)
	{
		for (int i = 0; i < b.length; i++)
		{
			if (a[i + from] != b[i])
			{
				return false;
			}
		}
		return true;
	}

	private char[]			recordTerminator;

	private char[]			columnTerminator;

	private Reader			reader;

	private char[]			buff	= new char[1000];

	private List<String>	columns	= new LinkedList<String>();

	private StringBuilder	cell	= new StringBuilder();

	private int				len		= 0;

	private int				pos		= 0;

	private boolean			begin	= false;

	private Record			next	= null;

	private long			id		= 1;

	protected void close()
	{
		try
		{
			this.getReader().close();
		}
		catch (Exception e)
		{
		}
	}

	protected char[] getBuff()
	{
		return buff;
	}

	protected char[] getColumnTerminator()
	{
		return columnTerminator;
	}

	public Reader getReader()
	{
		return reader;
	}

	protected char[] getRecordTerminator()
	{
		return recordTerminator;
	}

	@Override
	public boolean hasNext()
	{
		if (!begin)
		{
			try
			{
				this.next = this.nextRecord();
				begin = true;
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}
		return this.next != null;
	}

	protected Record newRecord(String[] data)
	{
		return new Record(id++, data);
	}

	@Override
	public Record next()
	{
		try
		{
			return this.next;
		}
		finally
		{
			try
			{
				this.next = this.nextRecord();
			}
			catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}
	}

	protected Record nextRecord() throws IOException
	{
		columns.clear();
		char[] colTerm = this.getColumnTerminator(), recTerm = this.getRecordTerminator();
		boolean colTermed = false, recTermed = false, iterated = false;
		int reads = -1;

		while (true)
		{
			colTermed = false;
			recTermed = false;

			try
			{
				reads = this.getReader().read(buff, len, buff.length - len);
			}
			catch (IOException e)
			{
				reads = -1;
			}

			if (reads > 0)
			{
				len += reads;
			}

			for (; pos < len; pos++)
			{
				iterated = true;

				if (pos <= len - colTerm.length && startWith(buff, pos, colTerm))
				{
					colTermed = true;
					break;
				}
				else if (pos <= len - recTerm.length && startWith(buff, pos, recTerm))
				{
					recTermed = true;
					break;
				}
				else if (pos > len - colTerm.length && samePrefix(buff, pos, colTerm))
				{
					break;
				}
				else if (pos > len - recTerm.length && samePrefix(buff, pos, recTerm))
				{
					break;
				}
			}

			cell.append(buff, 0, pos);

			if (colTermed)
			{
				pos += colTerm.length;
			}
			else if (recTermed)
			{
				pos += recTerm.length;
			}

			System.arraycopy(buff, pos, buff, 0, len - pos);
			len -= pos;
			pos = 0;

			if (colTermed)
			{
				columns.add(cell.toString());
				Tools.clearStringBuilder(cell);
			}
			else if (recTermed || (reads == -1 && pos >= len))
			{
				columns.add(cell.toString());
				Tools.clearStringBuilder(cell);
				break;
			}
		}

		if (recTermed)
		{
			return newRecord(columns.toArray(new String[columns.size()]));
		}
		else
		{
			try
			{
				if (iterated)
				{
					return newRecord(columns.toArray(new String[columns.size()]));
				}
				else
				{
					return null;
				}
			}
			finally
			{
				this.close();
			}
		}
	}

	protected void setBuff(char[] buff)
	{
		this.buff = buff;
	}

	public void setColumnTerminator(String columnTerminator)
	{
		this.columnTerminator = columnTerminator != null ? columnTerminator.toCharArray() : null;
	}

	public void setReader(Reader reader)
	{
		this.reader = reader;
	}

	public void setRecordTerminator(String recordTerminator)
	{
		this.recordTerminator = recordTerminator != null ? recordTerminator.toCharArray() : null;
	}
}
