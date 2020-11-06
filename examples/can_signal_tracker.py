#!/usr/bin/env python3
import csv
import sys
import functools


class Message():
  """Storage for a specific message"""

  def __init__(self, message_id, ts, data):
    self.id = message_id  # "BusId:CANId"
    self.ts = ts  # Time stamp of message
    self.data = data  # String representing data in Hex
    self.bit_changes = 0  # By default bit changes mask is all 0

  def bit_changes_from(self, ref_message):
    if ref_message is None:
      return 0
    return int(self.data, 16) ^ int(ref_message.data, 16)

  def with_bit_changes(self, ref_message):
    self.bit_changes = self.bit_changes_from(ref_message)
    return self

  def short_desc(self):
    return f'{self.ts:.4f} - {self.id} - {self.data}'

  def __repr__(self):
    return f'{self.ts:.4f} - {self.id} - {self.data} - {int(self.data, 16):064b} - {self.bit_changes:064b}'


class MessageTypeCollection():
  """A Collection of messages with the same id"""
  def __init__(self, message_id):
    self.id = message_id
    self.messages = []

  def add(self, message):
    self.messages.append(message)

  def messages_with_change(self):
    """Filter messages to remove instances that do not change in relation to previous messaege in collection.

    Returns:
        List: The reduce list of messages removing messages that don't change.
    """
    return functools.reduce(lambda ret, message: ret + [message]
                            if len(ret) == 0 or message.data != ret[-1].data else ret,
                            self.messages, [])

  @property
  def count(self):
    return len(self.messages)

  def __repr__(self):
    return f'{self.id} - Count: {self.count}'

  def desc(self):
    return functools.reduce(lambda ret, message: f'{ret}{message}\n',
                            self.messages,
                            "")


class MessageGroup():
  """A Group of messages between reference signals"""

  def __init__(self, ts, ref_message):
    self.ts = ts
    self.ref_message = ref_message
    self.message_type_collections = {}  # Keyed by message id

  @property
  def message_ids(self):
    return self.message_type_collections.keys()

  def add(self, message):
    if message.id not in self.message_type_collections:
      self.message_type_collections[message.id] = MessageTypeCollection(message.id)
    self.message_type_collections[message.id].add(message)

  def with_id(self, message_id):
    return self.message_type_collections.get(message_id, [])

  def appearing_once(self):
    return functools.reduce(lambda ret, collection: ret + collection.messages if collection.count == 1 else ret,
                            self.message_type_collections.values(),
                            [])

  def __repr__(self):
    return functools.reduce(lambda ret, collection: f'{ret}{collection}\n',
                            self.message_type_collections.values(),
                            f'\n******\nTS: {self.ts} - Message Types: {len(self.message_type_collections)}\n')

  def desc_for_message(self, message_id):
    return functools.reduce(lambda ret, item: f'{ret}{item[1].desc()}\n' if item[0] == message_id else ret,
                            self.message_type_collections.items(),
                            f'\n******\nTS: {self.ts} - Message Types: {len(self.message_type_collections)}\n')


class MessageGroups():
  """A collection of Messages Groups"""

  def __init__(self):
    self.groups = []

  def __repr__(self):
    return functools.reduce(lambda ret, group: f'{ret}{group}\n', self.groups, "")

  def desc_for_message(self, message_id):
    return functools.reduce(lambda ret, group: f'{ret}{group.desc_for_message(message_id)}\n', self.groups, "")

  @property
  def count(self):
    return len(self.groups)

  def load(self, filename, ref_message_id, ref_bus, start, end):
    """Given a CSV file, adds information about message IDs and their values."""
    with open(filename, 'r') as inp:
      reader = csv.reader(inp)
      header = next(reader, None)
      current_group = None

      if header[0] != 'time':
        print("Error: Source file must be a Cabana log file")
        sys.exit(0)

      for row in reader:
        if not len(row):
          continue

        time = float(row[0])
        if time < start or time > end:
          continue

        try:
          bus = row[2]
          int(bus)
        except Exception:
          continue

        if row[1].startswith('0x'):
          message_id = row[1][2:]  # remove leading '0x'
        else:
          message_id = hex(int(row[1]))[2:]  # old message IDs are in decimal

        if bus != ref_bus:
          continue

        mess_bus_id = '%s:%s' % (bus, message_id)
        if row[3].startswith('0x'):
          data = row[3][2:]  # remove leading '0x'
        else:
          data = row[3]
        message = Message(mess_bus_id, time, data)

        if message_id == ref_message_id:
          if current_group is not None:
            self.groups.append(current_group)
          current_group = MessageGroup(time, message)
        elif current_group is not None:
          current_group.add(message)


class MessageChangeTracker():
  def __init__(self, group_ts, message_id, ref_message, base_message):
    self.bit_comparers = [2**(i - 1) for i in range(64, 0, -1)]
    self.base_message = base_message
    self.ref_message = ref_message
    self.bit_changes_count = [0] * 64
    self.group_ts = group_ts
    self.id = message_id

  def track_changes_to(self, message):
    bit_change = message.bit_changes_from(self.base_message)
    changes = [int((comparer & bit_change) != 0) for comparer in self.bit_comparers]
    self.bit_changes_count = [sum(x) for x in zip(self.bit_changes_count, changes)]
    self.base_message = message

  def desc_with_less_than(self, count):
    count_str = functools.reduce(lambda ret, x: f'{ret}{x:02d}|' if x > 0 and x < count else f'{ret}--|',
                                 self.bit_changes_count, "")
    return f'{self.group_ts:.3f} - {self.ref_message.data} - {count_str}'

  def __repr__(self):
    count_str = functools.reduce(lambda ret, x: f'{ret}{x:02d}|', self.bit_changes_count, "")
    return f'{self.group_ts:.3f} - {self.ref_message.data} - {count_str}'


class MessageTracker():
  def __init__(self, message_id, group_count):
    self.id = message_id
    self.messages = {}  # Keyed by group ts
    self.ref_messages = {}  # Keyed by group ts
    self.group_count = group_count

  def add(self, message_group):
    collection = message_group.with_id(self.id)
    if message_group.ts not in self.messages:
      self.messages[message_group.ts] = []
      self.ref_messages[message_group.ts] = message_group.ref_message
    self.messages[message_group.ts] = self.messages[message_group.ts] + collection.messages_with_change()

  @property
  def count(self):
    return len(self.messages)

  @property
  def in_all(self):
    return len(self.messages.keys()) >= self.group_count

  @property
  def group_message_tuples(self):
    return [(group, message) for group, messages in self.messages.items() for message in messages]

  @property
  def group_message_tuples_with_bit_changes(self):
    tuples = []
    last = None
    for group, message in self.group_message_tuples:
      tuples.append((group, message.with_bit_changes(last)))
      last = message
    return tuples

  @property
  def in_all_groups_with_change(self):
    if not self.in_all:
      return False
    current_group = None
    group_changes = 0
    for group, message in self.group_message_tuples_with_bit_changes:
      if current_group is None:
        current_group = group
      if group == current_group:
        group_changes |= message.bit_changes
      else:
        if group_changes == 0:
          return False
        current_group = group
        group_changes = 0
    return group_changes > 0

  @property
  def group_change_trackers(self):
    prev_group_last_message = None
    change_trackers = {}
    for group, message in self.group_message_tuples:
      if group not in change_trackers:
        change_trackers[group] = MessageChangeTracker(group, self.id, self.ref_messages[group], prev_group_last_message)
      change_trackers[group].track_changes_to(message)
      prev_group_last_message = message
    return list(change_trackers.values())

  def bit_changes_str(self, count=1000):
    headers = [f'{(i - 1):02d}' for i in range(64, 0, -1)]
    return functools.reduce(lambda ret, change_tracker: f'{ret}{change_tracker.desc_with_less_than(count)}\n',
                            self.group_change_trackers[1:],
                            f'XXX.XXX - XXXXXXXXXXXXXXXX - {"|".join(headers)}\n')

  def __repr__(self):
    return functools.reduce(lambda ret, tuple: f'{ret}{tuple[0]:.4f} -- {tuple[1]}\n',
                            self.group_message_tuples_with_bit_changes,
                            "")


def TrackSignal(log_file, ref_message_id, ref_bus, time_range):
  """Track messaging between refernce signals on a specified time range
     to analyse change paterns consistent in between reference signals.

  Args:
      log_file (String): The file to parse. (csv)
      ref_message_id (Hex): The HEX message id to use as reference signal
      ref_bus (Int): The bus id to intercept signals on
      time_range (String): Time range represented in a string xxx.xx-yyy.yy
  """
  message_groups = MessageGroups()
  start, end = list(map(float, time_range.split('-')))
  message_groups.load(log_file, ref_message_id, ref_bus, start, end)

  trackers_dict = {}  # Keyed by message id

  for message_group in message_groups.groups:
    for message_id in message_group.message_ids:
      if message_id not in trackers_dict:
        trackers_dict[message_id] = MessageTracker(message_id, message_groups.count)
      trackers_dict[message_id].add(message_group)

  for tracker in filter(lambda tracker: tracker.in_all_groups_with_change, trackers_dict.values()):
    print(f'\n**** Tracker: {tracker.id}\n')
    print(tracker.bit_changes_str(4))
    input("Press Enter to continue...")


if __name__ == "__main__":
  if len(sys.argv) < 5:
    print('Usage:\n%s log.csv message_id bus_id <start>-<end>' % sys.argv[0])
    sys.exit(0)
  TrackSignal(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
