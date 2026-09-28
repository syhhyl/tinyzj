class Channel:

  REQ = "REQ"
  RSP = "RSP"
  DAT = "DAT"
  SNP = "SNP"


class SnpOpcode:

  SNP_SHARED = "SnpShared"
  SNP_UNIQUE = "SnpUnique"


class ReqOpcode:

  READ_NO_SNP = "ReadNoSnp"
  WRITE_NO_SNP_FULL = "WriteNoSnpFull"
  READ_SHARED = "ReadShared"
  READ_UNIQUE = "ReadUnique"


class RspOpcode:

  COMP = "Comp"
  DBID_RESP = "DBIDResp"
  COMP_DBID_RESP = "CompDBIDResp"
  COMP_ACK = "CompAck"
  SNP_RESP = "SnpResp"


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
