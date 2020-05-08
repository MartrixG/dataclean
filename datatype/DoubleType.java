package dataclean.datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DoubleType implements MetaType {
	private static String selfType = "Double";
	private Double value;
	private Double limitMin, limitMax;

	/**
	 * 默认值为0, 最大值为双精度浮点数的最大值, 最小值为双精度浮点数的最小值
	 */
	public DoubleType() {
		this.value = 0.0;
		this.limitMin = Double.MIN_NORMAL;
		this.limitMax = Double.MAX_VALUE;
	}

	@Override
	/**
	 * 将一个字符串解析成一个数字, 首先删除所有的空格和逗号","接着检查是否符合整数的正则表达式,接着转换为浮点数
	 */
	public void setValue(String value) throws IllegalClassFormatException {
		String regex = "[0-9.]+";
		Pattern p = Pattern.compile(regex);
		value = value.replace(" ", "");
		value = value.replace(",", "");
		Matcher m = p.matcher(value);
		if (m.matches()) {
			this.value = Double.valueOf(value);
		} else {
			throw new IllegalClassFormatException();
		}
	}

	@Override
	/**
	 * 检查value是否处于最小最大值的限制中
	 */
	public Boolean check() {
		return this.value <= this.limitMax && this.value >= this.limitMin;
	}

	@Override
	/**
	 * 设置的格式应满足文档中的要求, 然后解析成限制的最大值和最小值
	 */
	public void setFormat(String format) throws IllegalClassFormatException {
		format = format.replace(" ", "");
		format = format.replace(",", "");
		String[] min_max = format.split(";");
		String regex = "[0-9\\-.]+";
		Pattern p = Pattern.compile(regex);
		if (min_max[0].length() != 0) {
			Matcher m = p.matcher(min_max[0]);
			if (m.matches()) {
				this.limitMin = Double.valueOf(min_max[0]);
			} else {
				throw new IllegalClassFormatException();
			}
		}
		if (min_max[1].length() != 0) {
			Matcher m = p.matcher(min_max[1]);
			if (m.matches()) {
				this.limitMax = Double.valueOf(min_max[1]);
			} else {
				throw new IllegalClassFormatException();
			}
		}
	}

	@Override
	public String getType() {
		return selfType;
	}

	@Override
	public String toString() {
		return this.value.toString();
	}

	public Double getValue() {
		return this.value;
	}

	public void setRange(Double min, Double max) {
		this.limitMin = min;
		this.limitMax = max;
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeDouble(this.value);
		dataOutput.writeDouble(this.limitMax);
		dataOutput.writeDouble(this.limitMin);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.value = dataInput.readDouble();
		this.limitMax = dataInput.readDouble();
		this.limitMin = dataInput.readDouble();
	}
}
