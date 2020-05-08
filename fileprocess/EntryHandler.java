package dataclean.fileprocess;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * 使用java自带的SAX工具进行xml文件的处理
 */
public class EntryHandler extends DefaultHandler {
	private Map<String, String> tuples;
	private Map<String, String> format;
	private ArrayList<String> order;

	@Override
	public void startDocument() throws SAXException {
		super.startDocument();
		tuples = new HashMap<>();
		format = new HashMap<>();
		order = new ArrayList<>();
	}

	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		super.startElement(uri, localName, qName, attributes);
		if (qName != "root") {
			tuples.put(qName, attributes.getValue(0));
			if (attributes.getLength() > 1) {
				format.put(qName, attributes.getValue(1));
			}
			order.add(qName);
		}
	}

	public Map<String, String> getTuples() {
		return tuples;
	}

	public Map<String, String> getFormat() {
		return format;
	}

	public ArrayList<String> getOrder() {
		return order;
	}
}