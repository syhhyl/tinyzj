class Channel:

  REQ = "REQ"
  RSP = "RSP"
  DAT = "DAT"
  ERQ = "ERQ"


class ReqOpcode:

  READ_NO_SNP = "ReadNoSnp"
  WRITE_NO_SNP_FULL = "WriteNoSnpFull"


class RspOpcode:

  COMP = "Comp"
  DBID_RESP = "DBIDResp"


class DatOpcode:

  COMP_DATA = "CompData"
  NON_COPY_BACK_WRITE_DATA = "NonCopyBackWriteData"
