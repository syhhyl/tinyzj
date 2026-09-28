class Channel:

  REQ = "REQ"
  RSP = "RSP"
  DAT = "DAT"
  SNP = "SNP"


class SnpOpcode:

  SNP_SHARED = "SnpShared"
  SNP_UNIQUE = "SnpUnique"
  SNP_CLEAN_INVALID = "SnpCleanInvalid"
  SNP_CLEAN_SHARED = "SnpCleanShared"


class ReqOpcode:

  READ_NO_SNP = "ReadNoSnp"
  WRITE_NO_SNP_FULL = "WriteNoSnpFull"
  WRITE_NO_SNP_PTL = "WriteNoSnpPtl"
  ATOMIC_SWAP = "AtomicSwap"
  ATOMIC_COMPARE = "AtomicCompare"
  WRITE_UNIQUE_FULL = "WriteUniqueFull"
  WRITE_BACK_FULL = "WriteBackFull"
  READ_SHARED = "ReadShared"
  READ_UNIQUE = "ReadUnique"
  CLEAN_INVALID = "CleanInvalid"
  CLEAN_SHARED = "CleanShared"


class RspOpcode:

  COMP = "Comp"
  DBID_RESP = "DBIDResp"
  COMP_DBID_RESP = "CompDBIDResp"
  RETRY_ACK = "RetryAck"
  PCRD_GRANT = "PCrdGrant"
  COMP_ACK = "CompAck"
  SNP_RESP = "SnpResp"


class DatOpcode:

  COMP_DATA = "CompData"
  NON_COPY_BACK_WRITE_DATA = "NonCopyBackWriteData"
  SNP_RESP_DATA = "SnpRespData"
  COPY_BACK_WRITE_DATA = "CopyBackWriteData"


class RespErr:

  OK = "OK"
  EXOK = "EXOK"
  DERR = "DERR"
  NDERR = "NDERR"


class Resp:

  UC = "UC"
  UD = "UD"
  SC = "SC"
  I = "I"
