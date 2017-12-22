class MarketDataBase(object):

  def __init__(self, md_type, payload):
    self._md_type = md_type
    self._payload = payload

  @property
  def md_type(self):
    return self._md_type

  @property
  def payload(self):
    return self._payload
