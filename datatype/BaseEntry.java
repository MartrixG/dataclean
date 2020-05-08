package dataclean.datatype;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BaseEntry implements WritableComparable {
	public Map<String, MetaType> keyWords = new HashMap<>();
	public List<String> order = new ArrayList<>();

	public BaseEntry() {
	}

	/**
	 * 规定各个属性的类型（String, Integer, Boolean, Double）
	 *
	 * @param args Map类型, key 对应着属性, value 对应着类型
	 */
	public BaseEntry(Map<String, String> args) {
		for (Map.Entry<String, String> name : args.entrySet()) {
			switch (name.getValue()) {
				case "String":
					keyWords.put(name.getKey(), new StringType());
					break;
				case "Integer":
					keyWords.put(name.getKey(), new IntegerType());
					break;
				case "Boolean":
					keyWords.put(name.getKey(), new BooleanType());
					break;
				case "Double":
					keyWords.put(name.getKey(), new DoubleType());
					break;
				default:
					throw new IllegalStateException("Unexpected value: " + name.getValue());
			}
		}
	}

	/**
	 * 给一个条目设置各个属性的值
	 *
	 * @param keyValues 一个List 按照储存的顺序进行赋值, 参数为String类型, 按照文档进行解析
	 */
	public void setValues(List<String> keyValues) {
		for (int i = 0; i < order.size(); i++) {
			try {
				keyWords.get(order.get(i)).setValue(keyValues.get(i));
			} catch (IllegalClassFormatException e) {
				//System.out.println("条目：" + order.get(i) + "内容设置出错, 已设置为空");
			}
		}
	}

	/**
	 * 设置各个属性初始化的顺序
	 *
	 * @param order List 各个属性的名称
	 */
	public void setOrder(ArrayList<String> order) {
		this.order = order;
	}

	public void setFormat(Map<String, String> format) {
		for (Map.Entry<String, String> eachFormat : format.entrySet()) {
			try {
				keyWords.get(eachFormat.getKey()).setFormat(eachFormat.getValue());
			} catch (IllegalClassFormatException e) {
				//System.out.println("条目：" + eachFormat.getKey() + "格式设置出错, 已设置为默认格式");
			}
		}
	}

	/**
	 * 检查当前条目是否合法, 按照文档中的定义进行检查
	 *
	 * @return 合法返回true, 否则返回false
	 */
	public Boolean check() {
		Boolean flag = true;
		for (MetaType eachValues : keyWords.values()) {
			flag &= eachValues.check();
		}
		return flag;
	}

	@Override
	public String toString() {
		StringBuilder re = new StringBuilder();
		for (int i = 0; i < order.size() - 1; i++) {
			re.append(keyWords.get(order.get(i))).append(",");
		}
		re.append(keyWords.get(order.get(order.size() - 1)));
		return re.toString();
	}

	@Override
	public int hashCode() {
		return keyWords.hashCode();
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeInt(keyWords.size());
		for (int i = 0; i < keyWords.size(); i++) {
			dataOutput.writeUTF(order.get(i));
		}
		for (Map.Entry<String, MetaType> eachEntry : this.keyWords.entrySet()) {
			dataOutput.writeUTF(eachEntry.getKey());
			dataOutput.writeUTF(eachEntry.getValue().getType());
			eachEntry.getValue().write(dataOutput);
		}
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		Integer tot = dataInput.readInt();
		List<String> order = new ArrayList<>();
		for (int i = 0; i < tot; i++) {
			order.add(dataInput.readUTF());
		}
		this.order = order;
		for (int i = 0; i < tot; i++) {
			String key = dataInput.readUTF();
			String valueType = dataInput.readUTF();
			if ("String".equals(valueType)) {
				StringType value = new StringType();
				value.readFields(dataInput);
				this.keyWords.put(key, value);
			} else if ("Integer".equals(valueType)) {
				IntegerType value = new IntegerType();
				value.readFields(dataInput);
				this.keyWords.put(key, value);
			} else if ("Double".equals(valueType)) {
				DoubleType value = new DoubleType();
				value.readFields(dataInput);
				this.keyWords.put(key, value);
			} else if ("Boolean".equals(valueType)) {
				BooleanType value = new BooleanType();
				value.readFields(dataInput);
				this.keyWords.put(key, value);
			} else {
				throw new IllegalStateException("Unexpected value: " + valueType);
			}
		}
	}

	@Override
	public int compareTo(Object o) {
		return this.hashCode() < o.hashCode() ? 1 : 0;
	}
}