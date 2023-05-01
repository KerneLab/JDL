package org.kernelab.jdl;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class InsertBuilder
{
	public static final char	VARIABLE_PREFIX	= '@';

	private String				table;

	private String[]			loadItems;

	private Map<String, String>	setExprs;

	@SuppressWarnings("unchecked")
	public InsertTemplate build()
	{
		Map<String, Object> map = new LinkedHashMap<String, Object>();
		Map<Integer, Pattern> vars = new LinkedHashMap<Integer, Pattern>();

		String[] loads = this.getLoadItems();
		String item = null;
		for (int i = 0; i < loads.length; i++)
		{
			item = loads[i];
			if (item.charAt(0) == VARIABLE_PREFIX)
			{
				vars.put(i, Pattern.compile("\\Q" + item + "\\E\\b", Pattern.CASE_INSENSITIVE));
			}
			else
			{
				map.put(item, i);
			}
		}

		for (Entry<String, String> pair : this.getSetExprs().entrySet())
		{
			map.put(pair.getKey(), pair.getValue());
		}

		List<Integer> idx = new LinkedList<Integer>();
		StringBuilder cols = new StringBuilder();
		StringBuilder vals = new StringBuilder();
		Object val = null;
		Object[] find = null;
		for (Entry<String, Object> pair : map.entrySet())
		{
			if (cols.length() > 0)
			{
				cols.append(',');
			}
			cols.append(pair.getKey());
			val = pair.getValue();
			if (val instanceof Integer)
			{
				idx.add((Integer) val);
				if (vals.length() > 0)
				{
					vals.append(',');
				}
				vals.append('?');
			}
			else
			{
				find = findVarsIndex(val.toString(), vars);
				if (vals.length() > 0)
				{
					vals.append(',');
				}
				vals.append((String) find[0]);
				idx.addAll((List<Integer>) find[1]);
			}
		}

		return new InsertTemplate() //
				.setInsert("INSERT INTO " + this.getTable() + " (" + cols.toString() + ")" //
						+ " VALUES (" + vals.toString() + ")") //
				.setIndexes(idx);
	}

	protected Object[] findVarsIndex(String expr, Map<Integer, Pattern> vars)
	{
		TreeMap<Integer, Integer> found = new TreeMap<Integer, Integer>();

		Matcher m = null;
		for (Entry<Integer, Pattern> pair : vars.entrySet())
		{
			m = pair.getValue().matcher(expr);
			while (m.find())
			{
				found.put(m.start(), pair.getKey());
			}
			expr = pair.getValue().matcher(expr).replaceAll("?");
		}

		return new Object[] { expr, new LinkedList<Integer>(found.values()) };
	}

	public String[] getLoadItems()
	{
		return loadItems;
	}

	public Map<String, String> getSetExprs()
	{
		return setExprs;
	}

	public String getTable()
	{
		return table;
	}

	public InsertBuilder setLoadItems(String[] loadItems)
	{
		this.loadItems = loadItems;
		return this;
	}

	public InsertBuilder setSetExprs(Map<String, String> setExprs)
	{
		this.setExprs = setExprs;
		return this;
	}

	public InsertBuilder setTable(String table)
	{
		this.table = table;
		return this;
	}
}
