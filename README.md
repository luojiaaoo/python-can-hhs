# python-can-hhs
用于禾华盛CAN设备对python-can的支持

1. 修改python-can路径下的can/interfaces/__init__.py文件, 在BACKENDS字典中添加一行:

   ```
   "hhs": ("can.interfaces.hhs", "HhsBus"),
   ```

2. 将hhs文件夹拷贝到can/interfaces/文件夹下