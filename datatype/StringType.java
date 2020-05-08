package dataclean.datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringType implements MetaType {
	private static String selfType = "String";
	private String value;
	private String format;

	/**
	 * 初始化默认为空字符串, 格式为匹配任意字符串的正则表达式
	 */
	public StringType() {
		value = "";
		format = ".*";
	}

	@Override
	/**
	 * 检查value字符串是否符合format形式的正则表达式
	 */
	public Boolean check() {
		Pattern p = Pattern.compile(this.format);
		Matcher m = p.matcher(this.value);
		return m.matches();
	}

	@Override
	public void setFormat(String format) {
		this.format = format;
	}

	@Override
	public String toString() {
		return this.value;
	}

	@Override
	/**
	 * 设置value时删除头部和尾部的空格以及换行符
	 */
	public void setValue(String value) {
		this.value = value.trim();
	}

	@Override
	public String getType() {
		return selfType;
	}

	public String getValue() {
		return this.value;
	}

	public String getFormat() {
		return this.format;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(this.value);
		dataOutput.writeUTF(this.format);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.value = dataInput.readUTF();
		this.format = dataInput.readUTF();
	}
}
