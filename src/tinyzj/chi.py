class Channel:

  REQ = "REQ"
  RSP = "RSP"
  DAT = "DAT"
  ERQ = "ERQ"


class ReqOpcode:

  READ_NO_SNP = "ReadNoSnp"
  WRITE_NO_SNP_FULL = "WriteNoSnpFull"
  READ_SHARED = "ReadShared"


class RspOpcode:

  COMP = "Comp"
  DBID_RESP = "DBIDResp"
  COMP_ACK = "CompAck"


class DatOpcode:

  COMP_DATA = "CompData"
  NON_COPY_BACK_WRITE_DATA = "NonCopyBackWriteData"


class RespErr:

  OK = "OK"
  EXOK = "EXOK"
  DERR = "DERR"
  NDERR = "NDERR"


class Resp:

  UC = "UC"
  SC = "SC"
  I = "I"
