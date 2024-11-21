import sys
import ctypes
import logging
import os
import threading
from typing import Optional, List
from can import BusABC
from can.typechecking import CanFilters
from queue import Queue, Empty
from can.exceptions import CanOperationError, CanError
import can

log = logging.getLogger("can.hhs")


class Dev_Info(ctypes.Structure):
    _fields_ = [
        ("HW_Type", ctypes.c_char * 32),
        ("HW_Ser", ctypes.c_char * 32),
        ("HW_Ver", ctypes.c_char * 32),
        ("FW_Ver", ctypes.c_char * 32),
        ("MF_Date", ctypes.c_char * 32),
    ]


class Can_Config(ctypes.Structure):
    _fields_ = [
        ("baudrate", ctypes.c_uint),
        ("config", ctypes.c_char),
        ("Pres", ctypes.c_char),
        ("Tseg1", ctypes.c_char),
        ("Tseg2", ctypes.c_char),
        ("Model", ctypes.c_char),
        ("SJW", ctypes.c_char),
        ("Reserved1", ctypes.c_char),
        ("Reserved2", ctypes.c_char),
    ]


class CanFD_Config(ctypes.Structure):
    _fields_ = [
        ("NomBaud", ctypes.c_uint),
        ("DatBaud", ctypes.c_uint),
        ("NomPres", ctypes.c_char),
        ("NomTseg1", ctypes.c_char),
        ("NomTseg2", ctypes.c_char),
        ("NomSJW", ctypes.c_char),
        ("DatPres", ctypes.c_char),
        ("DatTseg1", ctypes.c_char),
        ("DatTseg2", ctypes.c_char),
        ("DatSJW", ctypes.c_char),
        ("Config", ctypes.c_char),
        ("Model", ctypes.c_char),
        ("Cantype", ctypes.c_char),
        ("Reserved", ctypes.c_char),
    ]


class Can_Msg(ctypes.Structure):
    _fields_ = [
        ("ID", ctypes.c_uint),
        ("TimeStamp", ctypes.c_uint),
        ("FrameType", ctypes.c_ubyte),
        ("DataLen", ctypes.c_ubyte),
        ("Data", ctypes.c_ubyte * 8),
        ("ExternFlag", ctypes.c_ubyte),
        ("RemoteFlag", ctypes.c_ubyte),
        ("BusSatus", ctypes.c_ubyte),
        ("ErrSatus", ctypes.c_ubyte),
        ("TECounter", ctypes.c_ubyte),
        ("RECounter", ctypes.c_ubyte),
    ]


class CanFD_Msg(ctypes.Structure):
    _fields_ = [
        ("ID", ctypes.c_uint),
        ("TimeStamp", ctypes.c_uint),
        ("FrameType", ctypes.c_ubyte),
        ("DLC", ctypes.c_ubyte),
        ("ExternFlag", ctypes.c_ubyte),
        ("RemoteFlag", ctypes.c_ubyte),
        ("BusSatus", ctypes.c_ubyte),
        ("ErrSatus", ctypes.c_ubyte),
        ("TECounter", ctypes.c_ubyte),
        ("RECounter", ctypes.c_ubyte),
        ("Data", ctypes.c_ubyte * 64),
    ]


class CanFD_Msg_ARRAY(ctypes.Structure):
    _fields_ = [('SIZE', ctypes.c_uint16), ('STRUCT_ARRAY', ctypes.POINTER(CanFD_Msg))]

    def __init__(self, num_of_structs):
        self.STRUCT_ARRAY = ctypes.cast((CanFD_Msg * num_of_structs)(), ctypes.POINTER(CanFD_Msg))
        self.SIZE = num_of_structs
        self.ADDR = self.STRUCT_ARRAY[0]


try:
    if sys.platform == "win32":
        __canlib = ctypes.windll.LoadLibrary(os.path.join(os.path.dirname(__file__), 'HCanbus'))
    else:
        __canlib = ctypes.cdll.LoadLibrary("libcanbus.so")
    log.info("loaded hhs's CAN library")
except Exception:
    log.warning("hhs canlib is unavailable.")
    __canlib = None


def __convert_status_to_int(result):
    if isinstance(result, int):
        return result
    else:
        return result.value


def __check_status(result, function, arguments):
    result = __convert_status_to_int(result)
    if result == -1:
        raise CanOperationError(f"{function.__name__} failed - rt:{result}", result)
    return result


def __get_canlib_function(func_name, argtypes=None, restype=None, errcheck=None):
    try:
        retval = getattr(__canlib, func_name)
    except AttributeError:
        raise NotImplementedError(
            f"This function {func_name} is not implemented in canlib"
        )
    else:
        retval.argtypes = argtypes
        retval.restype = restype
        if errcheck:
            retval.errcheck = errcheck
        return retval


if __canlib is not None:
    # 扫描CAN,CANFD设备
    CAN_ScanDevice = __get_canlib_function(
        "CAN_ScanDevice", restype=ctypes.c_int, errcheck=__check_status
    )
    # 获取CAN,CANFD类型
    CAN_GeDevType = __get_canlib_function(
        "CAN_GeDevType",
        argtypes=[ctypes.c_uint],
        restype=ctypes.c_int,
    )
    # 获取序列号
    CAN_ReadDevInfo = __get_canlib_function(
        "CAN_ReadDevInfo",
        argtypes=[ctypes.c_uint, ctypes.POINTER(Dev_Info)],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )
    # 初始化
    CANFD_Init = __get_canlib_function(
        "CANFD_Init",
        argtypes=[ctypes.c_uint, ctypes.POINTER(CanFD_Config)],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )
    # 添加过滤器
    CAN_SetFilter = __get_canlib_function(
        "CAN_SetFilter",
        argtypes=[
            ctypes.c_uint,
            ctypes.c_char,
            ctypes.c_char,
            ctypes.c_uint,
            ctypes.c_uint,
            ctypes.c_char,
        ],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )
    # 打开设备
    CAN_OpenDevice = __get_canlib_function(
        "CAN_OpenDevice",
        argtypes=[ctypes.c_uint],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )
    # 发送canfd报文
    CANFD_Transmit = __get_canlib_function(
        "CANFD_Transmit",
        argtypes=[
            ctypes.c_uint,
            ctypes.POINTER(CanFD_Msg),
            ctypes.c_uint,
            ctypes.c_int,
        ],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )
    # 接收缓冲区数量
    CANFD_GetReceiveNum = __get_canlib_function(
        "CANFD_GetReceiveNum",
        argtypes=[ctypes.c_uint],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )
    # 接收报文
    CANFD_Receive = __get_canlib_function(
        "CANFD_Receive",
        argtypes=[
            ctypes.c_uint,
            ctypes.POINTER(CanFD_Msg),
            ctypes.c_int,
            ctypes.c_int,
        ],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )
    # 关闭CAN通道
    CAN_CloseDevice = __get_canlib_function(
        "CAN_CloseDevice",
        argtypes=[ctypes.c_uint],
        restype=ctypes.c_int,
        errcheck=__check_status,
    )


class HhsBus(BusABC):
    DLC2BYTE_LEN = {0: 0, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6, 7: 7, 8: 8, 9: 12, 10: 16, 11: 20, 12: 24, 13: 32, 14: 48, 15: 64}
    BYTE_LEN2DLC = {j: i for i, j in DLC2BYTE_LEN.items()}

    send_canmsg = CanFD_Msg_ARRAY(100)
    read_canmsg = CanFD_Msg_ARRAY(2500)

    @staticmethod
    def __trans_data(data, dlc: int):
        if isinstance(data, int):
            data = data.to_bytes(length=dlc, byteorder='big')
        elif isinstance(data, bytearray) or isinstance(data, bytes):
            data = data
        else:
            data = list(data)
        return data

    def __init__(
            self,
            channel: int,
            fd: bool = False,
            bitrate: int = 500000,
            data_bitrate: int = 2000000,
            receive_own_messages: bool = False,
            can_filters: Optional[CanFilters] = None,
            retry_when_send_fail: bool = True,
            auto_reconnect: bool = True,
            only_listen_mode: bool = False,
            m120=False,
            **kwargs,
    ):
        self.channel = channel
        self.receive_own_messages = receive_own_messages
        self.queue_recv = Queue()
        self.queue_send = Queue()
        self._is_filtered = False
        # 打开总线
        CAN_OpenDevice(ctypes.c_uint(channel))
        # 总线配置
        canfd_config = CanFD_Config()
        canfd_config.NomBaud = bitrate
        canfd_config.DatBaud = data_bitrate
        canfd_config.NomPres = 0
        canfd_config.NomTseg1 = 0
        canfd_config.NomTseg2 = 0
        canfd_config.NomSJW = 0
        canfd_config.DatPres = 0
        canfd_config.DatTseg1 = 0
        canfd_config.DatTseg2 = 0
        canfd_config.DatSJW = 0
        canfd_config.Config = (m120 | auto_reconnect << 1 | retry_when_send_fail << 2)
        canfd_config.Model = (2 if only_listen_mode else 0)
        canfd_config.Cantype = 1 if fd else 0
        canfd_config.Reserved = 0
        # 初始化总线
        CANFD_Init(ctypes.c_uint(channel), canfd_config)
        # 硬件过滤器
        self.filter_num = None
        # 启动接收发送线程
        self.event_recv_send_batch = threading.Event()
        threading.Thread(None, target=self.__recv_send_batch, args=(self.event_recv_send_batch,)).start()
        super().__init__(
            channel=channel,
            can_filters=can_filters,
            **kwargs,
        )

    def send(self, msg: can.Message, timeout=None):
        self.queue_send.put(msg)

    def _recv_internal(self, timeout=None):
        try:
            msg = self.queue_recv.get(block=True, timeout=timeout)
        except Empty:
            return None, self._is_filtered or True
        else:
            return msg, self._is_filtered or True

    def __recv_send_batch(self, event):
        while not event.is_set():
            # 发送
            send_size = self.queue_send.qsize()
            if send_size:
                send_size = send_size if send_size <= 100 else 100  # 单次最多100帧

                for i in range(send_size):
                    msg: can.Message = self.queue_send.get()
                    data_msg = self.__trans_data(msg.data, msg.dlc)
                    self.send_canmsg.STRUCT_ARRAY[i].ID = msg.arbitration_id
                    self.send_canmsg.STRUCT_ARRAY[i].TimeStamp = 0
                    self.send_canmsg.STRUCT_ARRAY[i].FrameType = (
                            False | False << 1 | msg.is_fd << 2 | msg.bitrate_switch << 3 | 0 << 4 | 0 << 5 | self.receive_own_messages << 6
                    )
                    self.send_canmsg.STRUCT_ARRAY[i].DLC = self.BYTE_LEN2DLC[msg.dlc]
                    self.send_canmsg.STRUCT_ARRAY[i].ExternFlag = msg.is_extended_id
                    self.send_canmsg.STRUCT_ARRAY[i].RemoteFlag = msg.is_remote_frame
                    self.send_canmsg.STRUCT_ARRAY[i].BusSatus = 0
                    self.send_canmsg.STRUCT_ARRAY[i].ErrSatus = 0
                    self.send_canmsg.STRUCT_ARRAY[i].TECounter = 0
                    self.send_canmsg.STRUCT_ARRAY[i].RECounter = 0
                    for j in range(msg.dlc):
                        self.send_canmsg.STRUCT_ARRAY[i].Data[j] = data_msg[j]
                CANFD_Transmit(self.channel, ctypes.byref(self.send_canmsg.ADDR), send_size, 100)  # 默认100毫秒超时
            # 接收
            read_size = CANFD_Receive(self.channel, ctypes.byref(self.read_canmsg.ADDR), 2500, 100)  # 最多接收2500帧 默认100毫秒超时
            for i in range(read_size):
                msg_hhs = self.read_canmsg.STRUCT_ARRAY[i]
                frame_type = msg_hhs.FrameType
                # 发送失败的报文
                if bool(frame_type & 0b10):
                    continue
                len = self.DLC2BYTE_LEN[msg_hhs.DLC]
                msg = can.Message(
                    is_fd=True if frame_type & 0b100 else False,
                    timestamp=float(msg_hhs.TimeStamp) / 1000000,
                    is_extended_id=bool(msg_hhs.ExternFlag),
                    arbitration_id=msg_hhs.ID,
                    data=[msg_hhs.Data[j] for j in range(len)],
                    dlc=len,
                    channel=self.channel,
                    is_remote_frame=bool(msg_hhs.RemoteFlag),
                    is_rx=False if frame_type & 1 else True,
                )
                self.queue_recv.put(msg)
            event.wait(timeout=0.001)

    def shutdown(self):
        super().shutdown()
        self.event_recv_send_batch.set()
        CAN_CloseDevice(self.channel)

    def _apply_filters(self, filters):
        if filters is None:
            CAN_SetFilter(ctypes.c_uint(self.channel), 0, 0, 0, 0, 1)
            self._is_filtered = False
            return
        start_filter_num = 0 if self.filter_num is None else self.filter_num
        if start_filter_num + len(filters) > 14:
            raise CanError("Too many filters")
        for i, filter in enumerate(filters, start=start_filter_num):
            can_id = filter["can_id"]
            can_mask = filter["can_mask"]
            extended = 1 if filter.get("extended") else 0
            CAN_SetFilter(
                self.channel,
                i,
                1,  # 32位过滤器
                (can_id << 3) if extended else (can_id << 21),
                (can_mask << 3) if extended else (can_mask << 21),
                1  # 使能
            )
            self.filter_num = i
            self._is_filtered = True

    @staticmethod
    def _detect_available_configs():
        confs = []
        for i in range(CAN_ScanDevice()):
            fd = True if CAN_GeDevType(i) == 1 else False
            dev_info = Dev_Info()
            CAN_ReadDevInfo(i, dev_info)
            confs.append({'name': f'{dev_info.HW_Type.decode("utf8")}/channel{i}/SN:{dev_info.HW_Ser.decode("utf8")}', 'channel': i, 'fd': fd})
        return confs
