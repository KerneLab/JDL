package org.kernelab.jdl;

import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.kernelab.basis.Tools;

public class RecordParser implements Iterator<Record>
{
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

	private int				skip		= 0;

	private int				columnsNeed	= -1;

	private Reader			reader;

	private char[]			buff		= new char[1000];

	private List<String>	cols		= new LinkedList<String>();

	private StringBuilder	cell		= new StringBuilder();

	private int				len			= 0;

	private int				pos			= 0;

	private boolean			begin		= false;

	private Record			next		= null;

	private long			id			= 1;

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

	public int getColumnsNeed()
	{
		return columnsNeed;
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

	public int getSkip()
	{
		return skip;
	}

	@Override
	public boolean hasNext()
	{
		if (!begin)
		{
			begin = true;
			try
			{
				for (int i = 0; i < this.skip; i++)
				{
					if (this.nextRecord() == null)
					{
						break;
					}
				}
				this.next = this.nextRecord();
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
		cols.clear();
		char[] colTerm = this.getColumnTerminator(), recTerm = this.getRecordTerminator();
		int reads = -1, columns = this.getColumnsNeed();
		boolean colTermed = false, recTermed = false, iterated = false, needCols = columns != 0;

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

			if (needCols)
			{
				cell.append(buff, 0, pos);
			}

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
				if (needCols)
				{
					cols.add(cell.toString());
					Tools.clearStringBuilder(cell);
					if (columns > 0 && cols.size() >= columns)
					{
						needCols = false;
					}
				}
			}
			else if (recTermed || (reads == -1 && pos >= len))
			{
				if (needCols)
				{
					cols.add(cell.toString());
					Tools.clearStringBuilder(cell);
					if (columns > 0 && cols.size() >= columns)
					{
						needCols = false;
					}
				}
				break;
			}
		}

		if (recTermed)
		{
			return newRecord(cols.toArray(new String[cols.size()]));
		}
		else
		{
			try
			{
				if (iterated)
				{
					return newRecord(cols.toArray(new String[cols.size()]));
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

	@Override
	public void remove()
	{
	}

	protected void setBuff(char[] buff)
	{
		this.buff = buff;
	}

	public RecordParser setColumnsNeed(int columns)
	{
		this.columnsNeed = columns;
		return this;
	}

	public RecordParser setColumnTerminator(String columnTerminator)
	{
		this.columnTerminator = columnTerminator != null ? columnTerminator.toCharArray() : null;
		return this;
	}

	public RecordParser setReader(Reader reader)
	{
		this.reader = reader;
		return this;
	}

	public RecordParser setRecordTerminator(String recordTerminator)
	{
		this.recordTerminator = recordTerminator != null ? recordTerminator.toCharArray() : null;
		return this;
	}

	public RecordParser setSkip(int skip)
	{
		this.skip = skip;
		return this;
	}
}
