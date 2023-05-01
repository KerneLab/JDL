package org.kernelab.jdl;

import java.io.PrintStream;

public class Record
{
	public final long		id;

	public final String[]	data;

	public Record(long id, String[] data)
	{
		this.id = id;
		this.data = data;
	}

	public void printError(PrintStream err, Exception ex)
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
