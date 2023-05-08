package org.kernelab.jdl;

import java.util.List;

public class InsertTemplate
{
	private String[]	items;

	private String		table;

	private String		target;

	private String		values;

	private String		insert;

	private int[]		indexes;

	public int[] getIndexes()
	{
		return indexes;
	}

	public String getInsert()
	{
		if (insert == null)
		{
			insert = "INSERT INTO " + this.getTable() + " (" + this.getTarget() + ") VALUES (" + this.getValues() + ")";
		}
		return insert;
	}

	public String getInsert(int rows)
	{
		String head = "INSERT INTO " + this.getTable() + " (" + this.getTarget() + ") VALUES";

		String values = "(" + this.getValues() + ")";

		StringBuilder buff = new StringBuilder(head.length() + (values.length() + 1) * rows);

		buff.append(head);

		for (int i = 0; i < rows; i++)
		{
			if (i == 0)
			{
				buff.append(' ');
			}
			else
			{
				buff.append(',');
			}
			buff.append(values);
		}

		return buff.toString();
	}

	public String[] getItems()
	{
		return items;
	}

	public String getTable()
	{
		return table;
	}

	public String getTarget()
	{
		return target;
	}

	public String getValues()
	{
		return values;
	}

	protected InsertTemplate setIndexes(int[] indexes)
	{
		this.indexes = indexes;
		return this;
	}

	protected InsertTemplate setIndexes(List<Integer> indexes)
	{
		int[] idx = null;

		if (indexes != null)
		{
			idx = new int[indexes.size()];
			int j = 0;
			for (Integer i : indexes)
			{
				idx[j++] = i;
			}
		}

		return this.setIndexes(idx);
	}

	protected InsertTemplate setItems(String[] items)
	{
		this.items = items;
		return this;
	}

	protected InsertTemplate setTable(String table)
	{
		this.table = table;
		this.insert = null;
		return this;
	}

	protected InsertTemplate setTarget(String target)
	{
		this.target = target;
		this.insert = null;
		return this;
	}

	protected InsertTemplate setValues(String values)
	{
		this.values = values;
		this.insert = null;
		return this;
	}
}
