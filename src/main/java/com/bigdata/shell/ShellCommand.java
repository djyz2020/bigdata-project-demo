package com.bigdata.shell;

import java.io.IOException;

public class ShellCommand {
	
	public static void main(String[] args) {
		String command = "";
		try {
			Process ps = Runtime.getRuntime().exec(command);
			ps.getInputStream();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
