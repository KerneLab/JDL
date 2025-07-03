package org.kernelab.jdl;

import java.util.Map;

public interface Cryptor
{
	public String decrypt(String text, Map<String, String> prop) throws Exception;
}
