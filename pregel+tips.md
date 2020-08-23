# Pregel+ 配环境 TIPS

来源：http://www.cse.cuhk.edu.hk/pregelplus/deploy-hadoop2.html



### Hadoop 2 + dependencies

（我是按hadoop 2 的版本来的 ，hadoop 1 应该也可以，但我没试过。）

http://www.cse.cuhk.edu.hk/pregelplus/deploy-hadoop2.html

##### jdk

教程里装了jdk7，我装了jdk8也ok，就是后面路径要记得改成jdk8版本的。

##### SSH configuration

如果只在自己电脑上跑，那只要把`/etc/hosts`里的localhost改成master就可以了（因为示例代码hashmin写的是连master）

##### Hadoop

教程里是Hadoop 2.6.1，其他版本也是可以的，就是路径要记得手动改一下。

同样在自己单机上跑的话，`$HADOOP_HOME/etc/hadoop/slaves`就不用写了，`$HADOOP_HOME/etc/hadoop/hdfs-site.xml`里的replication也不用设为3，1就可以了。

全部装好后，以后只要`$HADOOP_HOME/sbin/start-dfs.sh`就可以启动Hadoop了。



### 命令行跑HashMin

http://www.cse.cuhk.edu.hk/pregelplus/console.html

如果在单机上跑，`hashmin`文件夹中的`conf`只需要设置master就行。

也可以跳掉**Program Distribution**这一步。

HDFS里看东西和命令行挺像的，`hadoop fs -ls /` 相当于`ls /`, `hadoop fs -cat /xx.txt` 相当于 `cat /**.txt`，其它命令可以自己搜一下。



### Eclipse IDE

1. 如果报错`xxx.so`找不到或无法打开的话，说明动态链接没设置好，在命令行输入`sudo ldconfig`.

2. Project Properties那里，教程给的是hadoop1版本的，hadoop2得这样填：

   Choose **[C/C++ Build]−>[Settings]**, then choose **[GCC C++ Compiler]−>[Includes]**, and add the following three paths to **Include paths (-l)**:

   ```bash
   $HADOOP_HOME/include
   $JAVA_HOME/include
   $JAVA_HOME/include/linux
   ```

   (Use your absolute paths of `$HADOOP_HOME` and `$JAVA_HOME` instead of themselves，不知道的话可以用echo命令看，下同)

   Choose **[GCC C++ Linker]−>[Libraries]**, and add the following path to **Library serch path (-L)**:

   ```bash
   $HADOOP_HOME/lib/native
   ```

   Choose **[GCC C++ Linker]−>[Miscellaneous]**, and add the following path to **Other objects**:

   ```bash
   $HADOOP_HOME/lib/native/libhdfs.a
   $JAVA_HOME/jre/lib/amd64/server/libjvm.so
   ```

   （最保险的做法就是在命令行看一下这些地址是否都存在）

3. build project的时候，如果报错什么东西不能用`-PIE`编译，在linker--miscallaneous里，在link flag里加一个`-no-pie`.

4. run的时候，如果报错`libjvm.so`找不到或无法打开的话，在Run Configurations的环境变量里把`$LD_LIBRARY_PATH`也加进去。（我就是这样……不知道为啥总是动态链接失败）

5. 最后跑前记得启动hdfs。（`$HADOOP_HOME/sbin/start-dfs.sh`）
