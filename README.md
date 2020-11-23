# wrapped_api

主要是自己写的一些接口性质的代码，放在这里，以供后续复用

boto3_api: 基于AWS (Amazon Web Services)提供的`对象存储服务` Amazon S3
           (Simple Storage Serivce)的访问接口boto3进一步封装的API，用于
		   对Bucket执行遍历和删除操作等。

		   对象存储访问的权限控制涉及：access_key ()、secret_key()、以及
		   end_point()。需要知道这三项才能够访问相应的存储对象。

		   对象存储系统和文件系统并不一样，基本单位是Bucket，桶内都是对象，
		   虽然桶内对象的名称可以使用类似于文件系统的前缀，可以使用`Bucket
		   +Prefix`	的方式进行精准的访问。
