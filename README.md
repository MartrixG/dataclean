src文件夹内是源代码以及注释，Main.java为主函数入口

test_data文件夹内的HOS和TAX文件为原始数据和jar文件，order.txt文件为使用hadoop运行指定jar的指令。更换环境后需要修改指令中的绝对路径。

origina_jar为personalclass的check方法没有任何操作的原始jar文件。

HOS和TAX文件夹内的.csv文件为原始的表格文件，实际处理时使用删除了.csv头一行的.txt文件。environment和profile文件需要修改所有所需的文件路径为绝对路径。dataclean.jar为需要执行的jar包。

如果需要自定义personalclass，可以按照实验报告中2.3.4 客户自定义类介绍的办法修改。