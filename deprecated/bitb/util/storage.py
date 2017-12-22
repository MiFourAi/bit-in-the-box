class SynchronousDiskStorage(object):

  def __init__(self, capacity, dumper):
    assert dumper is not None
    self._capacity = max(capacity, 100)
    self._dumper = dumper
    self._dump_n = 0
    self._items = []

  def add_item(self, item):
    self._items.append(item)
    if len(self._items) >= self._capacity:
      self._run_dump()

  def force_dump(self):
    self._run_dump()

  def _run_dump(self):
    items = self._items
    self._items = []
    self._dump_n += 1
    self._dumper.dump(items, self._dump_n)
