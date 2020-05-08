package dataclean.datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;

public class BooleanType implements MetaType {
	private static String selfType = "BooleanType";
	private Boolean value;
	private String format;

	public BooleanType() {
		value = true;
		format = "*";
	}

	@Override
	/**
	 * 设置布尔型数据可以用true, 1表示"真" false, 0表示"假"
	 */
	public void setValue(String value) throws IllegalClassFormatException {
		value = value.toLowerCase();
		switch (value) {
			case "true":
			case "1":
				this.value = true;
				break;
			case "false":
			case "0":
				this.value = false;
				break;
			default:
				throw new IllegalClassFormatException();
		}
	}

	@Override
	/**
	 * 设置format为传入的参数
	 */
	public void setFormat(String format) {
		this.format = format.toLowerCase().trim();
	}

	@Override
	public String getType() {
		return selfType;
	}

	@Override
	/**
	 * 若format为通配符*表示不需要对真假进行检查, 直接返回true, 否则检查是否匹配
	 */
	public Boolean check() {
		if (this.format.equals("*")) {
			return true;
		}
		return (this.format.equals("true") && this.value) || (this.format.equals("false") && this.value == false);
	}

	public Boolean getValue() {
		return this.value;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeBoolean(this.value);
		dataOutput.writeBytes(format);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.value = dataInput.readBoolean();
		this.format = dataInput.readUTF();
	}
}
