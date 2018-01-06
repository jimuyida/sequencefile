# sequencefile

为了把一个文件夹下面所有的小文件合并成一个hadoop 格式的 sequence file，方便存储抽取

sequence file格式的解析参考：http://github.com/colinmarc/sequencefile

## hadoop sequence file 格式创建

```go

	seq := &SEQWRITER.SequenceFile{}
	err := seq.Dir2Seq("E:\\4032204Y(照片待合成)","D:\\target.seq");
	if err != nil {
		fmt.Println("创建seq文件失败:",err)
	} else {
		fmt.Println("创建seq文件成功")
	}
```

