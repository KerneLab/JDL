package org.kernelab.jdl;

import java.util.List;

public class InsertTemplate
{
	private String[]	items;

	private String		insert;

	private int[]		indexes;

	public int[] getIndexes()
	{
		return indexes;
	}

	public String getInsert()
	{
		return insert;
	}

	public String[] getItems()
	{
		return items;
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

	protected InsertTemplate setInsert(String insert)
	{
		this.insert = insert;
		return this;
	}

	protected InsertTemplate setItems(String[] items)
	{
		this.items = items;
		return this;
	}
}
