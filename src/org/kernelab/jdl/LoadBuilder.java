package org.kernelab.jdl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.kernelab.basis.JSON;
import org.kernelab.basis.Tools;
import org.kernelab.basis.Variable;

public class LoadBuilder
{
	public static String restoreText(String text)
	{
		if (Tools.notNullOrEmpty(text))
		{
			String result = null;

			if (text.endsWith("'"))
			{
				String sub = text.substring(0, text.length() - 1);

				if (sub.startsWith("'"))
				{
					result = JSON.RestoreStringContent(sub.substring(1));
				}
				else if (sub.startsWith("X'") || sub.startsWith("x'"))
				{
					String code = sub.substring(2);
					result = "";
					for (int i = 0; i < code.length() - 1; i += 2)
					{
						result += (char) Integer.parseInt(code.substring(i, i + 2), 16);
					}
				}
				else
				{
					result = JSON.RestoreStringContent(text);
				}
			}
			else
			{
				result = text;
			}

			return result;
		}
		else
		{
			return null;
		}
	}

	private String				filePath;

	private String				table;

	private String				charSet;

	private String				columnTerminator;

	private String				recordTerminator;

	private Integer				skip;

	private String[]			itemList;

	private Map<String, String>	setMap;

	public RecordParser buildParser() throws IOException
	{
		InputStream is = new FileInputStream(new File(this.getFilePath()));
		Reader in = new InputStreamReader(is, this.getCharSet());
		return new RecordParser() //
				.setReader(in) //
				.setColumnTerminator(this.getColumnTerminator()) //
				.setRecordTerminator(this.getRecordTerminator()) //
				.setSkip(this.getSkip() != null ? this.getSkip() : 0) //
		;
	}

	protected Map<String, String> buildSetMap(String setList)
	{
		Map<String, String> map = new LinkedHashMap<String, String>();

		String regex = "(?:\\s*,\\s*|^\\s*)([^=]+?)\\s*?=";
		Matcher m = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(setList);

		String lastKey = null;
		int lastEnd = -1;
		while (m.find())
		{
			if (lastEnd != -1)
			{
				map.put(lastKey, setList.substring(lastEnd, m.start()));
			}
			lastEnd = m.end();
			lastKey = m.group().replaceFirst(regex, "$1");
		}

		map.put(lastKey, setList.substring(lastEnd, setList.length()));

		return map;
	}

	public InsertTemplate buildTemplate()
	{
		return new InsertBuilder() //
				.setTable(this.getTable()) //
				.setLoadItems(this.getItemList()) //
				.setSetExprs(this.getSetMap()) //
				.build();
	}

	protected String[] extract(String cmd, String regex, String replace)
	{
		Matcher m = Pattern.compile(regex, Pattern.CASE_INSENSITIVE).matcher(cmd);

		if (!m.find())
		{
			return new String[] { cmd, null };
		}

		return new String[] { cmd.substring(0, m.start()) + cmd.substring(m.end()),
				m.reset(m.group()).replaceFirst(replace) };
	}

	protected String getCharSet()
	{
		return charSet;
	}

	protected String getColumnTerminator()
	{
		return columnTerminator;
	}

	protected String getFilePath()
	{
		return filePath;
	}

	protected String[] getItemList()
	{
		return itemList;
	}

	protected String getRecordTerminator()
	{
		return recordTerminator;
	}

	protected Map<String, String> getSetMap()
	{
		return setMap;
	}

	protected Integer getSkip()
	{
		return skip;
	}

	protected String getTable()
	{
		return table;
	}

	protected void log(String log)
	{
		// Tools.debug(log);
	}

	public LoadBuilder resolve(String cmd)
	{
		String[] res = extract(cmd, "^\\s*LOAD\\s+DATA\\s+LOCAL\\s+INFILE\\s+'(.+?)'\\s+", "$1");

		String filePath = res[1];
		if (filePath == null)
		{
			throw new IllegalArgumentException("File path not given in command: " + cmd);
		}
		filePath = JSON.RestoreStringContent(filePath);
		this.setFilePath(filePath);
		log("filePath: " + filePath);

		res = extract(res[0], "^\\s*INTO\\s+TABLE\\s+(\\S+?)\\s+", "$1");
		String table = res[1];
		if (table == null)
		{
			throw new IllegalArgumentException("Table name not given in command: " + cmd);
		}
		this.setTable(table);
		log("table: " + table);

		res = extract(res[0], "^\\s*CHARACTER\\s+SET\\s+(\\S+?)\\s+", "$1");
		String charSet = res[1];
		if (charSet == null)
		{
			throw new IllegalArgumentException("Character set not given in command: " + cmd);
		}
		this.setCharSet(charSet);
		log("charSet: " + charSet);

		res = extract(res[0], "^\\s*(?:FIELDS|COLUMNS)\\s+TERMINATED\\s+BY\\s+(.+?')\\s+(?:(?=LINES\\s+)|$)", "$1");
		String colSplit = restoreText(res[1]);
		if (colSplit == null)
		{
			throw new IllegalArgumentException("Column terminator not given in command: " + cmd);
		}
		this.setColumnTerminator(colSplit);
		log("colsSplit: " + colSplit);

		res = extract(res[0], "^\\s*(?:LINES|ROWS)\\s+TERMINATED\\s+BY\\s+(.+?')\\s+(?:(?=[(])|(?=IGNORE\\s+)|$)",
				"$1");
		String recSplit = restoreText(res[1]);
		if (recSplit == null)
		{
			throw new IllegalArgumentException("Record terminator not given in command: " + cmd);
		}
		this.setRecordTerminator(recSplit);
		log("recSplit: " + recSplit);

		res = extract(res[0], "^\\s*IGNORE\\s+(\\d+)\\s+(?:LINES|ROWS)\\s+", "$1");
		Integer skip = Variable.asInteger(res[1]);
		this.setSkip(skip);
		log("skip: " + skip);

		res = extract(res[0], "^\\s*\\(([^)]+?)\\)(?:\\s+|$)", "$1");
		String colsList = res[1];
		if (colsList == null)
		{
			throw new IllegalArgumentException("Columns list not given in command: " + cmd);
		}
		String[] cols = colsList.split("\\s*,\\s*", -1);
		this.setItemList(cols);
		log("cols: " + Tools.jointStrings(", ", cols));

		res = extract(res[0], "^\\s*SET\\s+(.+?)\\s*$", "$1");
		String setList = res[1];
		Map<String, String> setMap = this.buildSetMap(setList);
		this.setSetMap(setMap);
		log("setMap: " + new JSON().attrAll(setMap).toString());

		return this;
	}

	protected void setCharSet(String charSet)
	{
		this.charSet = charSet;
	}

	protected void setColumnTerminator(String columnTerminator)
	{
		this.columnTerminator = columnTerminator;
	}

	protected void setFilePath(String filePath)
	{
		this.filePath = filePath;
	}

	protected void setItemList(String[] columnList)
	{
		this.itemList = columnList;
	}

	protected void setRecordTerminator(String recordTerminator)
	{
		this.recordTerminator = recordTerminator;
	}

	protected void setSetMap(Map<String, String> setMap)
	{
		this.setMap = setMap;
	}

	protected void setSkip(Integer skip)
	{
		this.skip = skip;
	}

	protected void setTable(String table)
	{
		this.table = table;
	}
}
