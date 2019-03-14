package com.amazonaws.streamdataingestion;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class StreamingUtils {

	public static List<String> generateWebserverLogEntries(int entryCount) {

		List<String> entries = new ArrayList<String>();
		for (int i = 0; i < entryCount; i++) {
			entries.add(generateWebserverLogEntry());
		}

		return entries;
	}

	public static String generateWebserverLogEntry() {

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();

		// Current date
		String currentDate = dateFormat.format(date);

		// Random ip address
		Random r = new Random();
		String ip = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);

		// Request
		List<String> requests = Arrays.asList("/images/dog.jpg", "/index.html", "/contact/info.html",
				"/images/cat.html");
		String request = "GET " + requests.get(r.nextInt(requests.size()));

		// HTTP Answer
		final int httpAnswer = 200;

		// Visitor browser
		List<String> agentBrowsers = Arrays.asList("Mozilla/4.0", "Mozilla/3.0", "Chrome", "Internet Explorer 11");
		String agentBrowser = agentBrowsers.get(r.nextInt(agentBrowsers.size()));

		return String.format("%s\t%s\t%s\t%d\t%s\n", currentDate, ip, request, httpAnswer, agentBrowser);
	}
}
