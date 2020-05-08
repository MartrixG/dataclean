package dataclean.datatype;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.instrument.IllegalClassFormatException;

public interface MetaType {
	/**
	 * 设置当前属性的值
	 *
	 * @param value 一个需要解析的字符串, 具体解析方式按照继承的类进行处理
	 * @throws IllegalClassFormatException 若字符串无法解析则抛出非法类型格式异常
	 */
	public void setValue(String value) throws IllegalClassFormatException;

	/**
	 * 获得该属性的类型
	 *
	 * @return 一个字符串表示属性(String, Integer, Double, Boolean)
	 */
	public String getType();

	/**
	 * 对进行一个属性的值的检查, 具体检查方式按照继承的类进行处理
	 *
	 * @return
	 */
	public Boolean check();

	/**
	 * 设置一个属性的限制条件
	 *
	 * @param format 一个字符串, 解析成各个类型的限制条件, 具体解析方式按照继承的类进行处理
	 * @throws IllegalClassFormatException 若字符串无法解析则抛出非法类型格式异常
	 */
	public void setFormat(String format) throws IllegalClassFormatException;

	/**
	 * 序列化当前类型
	 *
	 * @param dataOutput 输出流, 用于写操作
	 * @throws IOException 若输出产生异常抛出IO异常
	 */
	public void write(DataOutput dataOutput) throws IOException;

	/**
	 * 反序列化
	 *
	 * @param dataInput 输入流, 用于读操作
	 * @throws IOException 若输入产生异常抛出IO异常
	 */
	public void readFields(DataInput dataInput) throws IOException;

	/**
	 * 获得当前属性储存的具体值
	 *
	 * @return 具体值
	 */
	public Object getValue();
}
