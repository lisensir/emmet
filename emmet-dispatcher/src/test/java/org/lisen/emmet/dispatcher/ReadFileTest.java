package org.lisen.emmet.dispatcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Test;

import junit.framework.Assert;

public class ReadFileTest {

	@Test
	public void test() {
		
		//InputStream in = Object.class.getResourceAsStream("/test.json");
		InputStream in = this.getClass().getResourceAsStream("/test.json");
		//InputStream in = Object.class.getClassLoader().getResourceAsStream("test.json");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		StringBuffer buffer = new StringBuffer();
		String line = "";
		try {
			while((line = br.readLine()) != null) {
				buffer.append(line);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("json string = "+buffer.toString());
		Assert.assertNotNull(buffer);
	}
	
	

}
