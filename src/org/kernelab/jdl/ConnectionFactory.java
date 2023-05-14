package org.kernelab.jdl;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class ConnectionFactory
{
	public static final int		REBALANCE_NONE		= 0;

	public static final int		REBALANCE_PICKONE	= 1;

	public static final int		REBALANCE_REARRANGE	= 2;

	protected final String		url;

	protected final String		user;

	protected final String		password;

	protected final Properties	properties;

	public ConnectionFactory(String url, String user, String password)
	{
		this.url = url;
		this.user = user;
		this.password = password;
		this.properties = new Properties();
		this.properties.put("user", this.user);
		this.properties.put("password", this.password);
	}

	protected Properties getProperties()
	{
		return properties;
	}

	protected String getUrl()
	{
		return url;
	}

	public synchronized Connection newConnection() throws SQLException
	{
		return DriverManager.getConnection(this.getUrl(), this.getProperties());
	}
}
