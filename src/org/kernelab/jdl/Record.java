package org.kernelab.jdl;

import java.io.PrintWriter;
import java.nio.charset.Charset;

public class Record
{
	public final long		id;

	public final String[]	data;

	public Record(long id, String[] data)
	{
		this.id = id;
		this.data = data;
	}

	public long bytes(Charset cs)
	{
		long bytes = 0L;
		for (String d : this.data)
		{
			bytes += d.getBytes(cs).length;
		}
		return bytes;
	}

	public void printError(PrintWriter err, Exception ex)
	{
		err.println("Rec#" + id + ": " + ex.getLocalizedMessage());
	}

	@Override
	public String toString()
	{
		return toString(", ");
	}

	public String toString(String split)
	{
		StringBuilder buf = new StringBuilder("Rec#" + id + ": ");

		boolean first = true;

		for (String d : data)
		{
			if (first)
			{
				first = false;
			}
			else
			{
				buf.append(split);
			}
			buf.append(d);
		}

		return buf.toString();
	}
}
