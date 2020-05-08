package dataclean.fileprocess;

import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 * 使用java自带的SAX工具进行xml文件的处理
 */
public class XmlProcess {
	public static Map<String, String> getPath(String environmentPath) {
		SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
		SAXParser saxParser;
		try {
			saxParser = saxParserFactory.newSAXParser();
			EntryHandler handler = new EntryHandler();
			saxParser.parse(new File(environmentPath), handler);
			Map<String, String> path = handler.getTuples();
			return path;
		} catch (SAXException | ParserConfigurationException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
