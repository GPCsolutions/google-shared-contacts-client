#!/usr/bin/python
#
# Copyright (C) 2008 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import csv
import getpass
import itertools
import operator
import optparse
import sys
import atom
import gdata.contacts
import gdata.contacts.service


# Maximum number of operations in batch feeds.
BATCH_CHUNK_SIZE = 100

# Number of contacts to retrieve at once in ContactsManager.GetAllContacts()
READ_CHUNK_SIZE = 1000

GDATA_VER_HEADER = 'GData-Version'

# Supported actions, for the "Action" CSV column, in lowercase.
ACTION_ADD = 'add'
ACTION_UPDATE = 'update'
ACTION_DELETE = 'delete'
DEFAULT_ACTION = ACTION_ADD
ACTIONS = (ACTION_ADD, ACTION_UPDATE, ACTION_DELETE)


def Chunks(iterable, size):
  """Splits an iterable into chunks of the given size.

  itertools.chain(*Chunks(iterable, size)) equals iterable.

  Examples:
    Chunks(range(1, 7), 3) yields [1,2,3], [4,5,6]
    Chunks(range(1, 8), 3) yields [1,2,3], [4,5,6], [7]
    Chunks([], 3) yields nothing

  Args:
    iterable: The iterable to cut into chunks.
    size: The size of all chunks, except the last chunk which may be smaller.

  Yields:
    Lists of elements, each of them having the given size, except the last one.
  """
  chunk = []
  for elem in iterable:
    chunk.append(elem)
    if len(chunk) == size:
      yield chunk
      chunk = []
  if chunk:
    yield chunk


def Log(line):
  """Prints a line to the standard output and flush it."""
  print line
  sys.stdout.flush()


def GetContactShortId(contact_entry):
  """Retrieves the short ID of a contact from its GData entry.

  The short ID of an entry is the GData entry without the URL prefix, e.g.
  "1234" for "http://www.google.com/m8/feeds/contacts/domain.tld/1234".
  """
  full_id = contact_entry.id.text
  return full_id[full_id.rfind('/') + 1:]


def PrintContact(index, contact_entry, contact_id=None, more=''):
  """Prints the index, name and email address of a contact.

  Args:
    index: A zero-based index, printed as one-based.
    contact_entry: The gdata.contacts.ContactEntry instance to print.
    more: Additional text to append to the printed line.
  """
  primary_email = None
  display_name = None
  if contact_entry:
    for email in contact_entry.email:
      if email.primary and email.primary == 'true':
        primary_email = email.address
        break
    display_name = contact_entry.title.text
    if contact_id:
      contact_id = '%s - ' % contact_id
    else:
      contact_id = ''
  Log('%5d) %s%s - %s%s' % (
      index + 1, contact_id, display_name, primary_email, more))


class ImportStats(object):
  """Tracks the progression of an import.

  Each operation (add, update, delete) has two counters: done and total.
  Users can set them by calling, for instance:
  counters.added_done += 1
  counters.updated_total += 1

  The class has a human-readable string representation.
  """

  COUNTER_PREFIXES = ('added', 'updated', 'deleted')
  COUNTERS = sum(map(lambda prefix: ['%s_done' % prefix, '%s_total' % prefix],
                     COUNTER_PREFIXES), [])

  def __init__(self):
    for counter in self.COUNTERS:
      setattr(self, counter, 0)

  def Add(self, other):
    for counter in self.COUNTERS:
      setattr(self, counter, getattr(self, counter) + getattr(other, counter))

  def __str__(self):
    bits = []
    all_done = 0
    all_total = 0
    for prefix in self.COUNTER_PREFIXES:
      done = getattr(self, '%s_done' % prefix)
      total = getattr(self, '%s_total' % prefix)
      all_done += done
      all_total += total
      bits.append('%s: %d/%d' % (prefix, done, total))
    errors = all_total - all_done
    bits.append('errors: %d' % errors)
    return ' - '.join(bits)


class ContactsManager(object):

  """Provides high-level operations for a contact list.

  Services:
  - importing contacts from an MS Outlook CSV file
  - exporting contacts to an MS Outlook CSV file
  - deleting all contacts

  The contact list is typically a domain contact list, that is the user is
  logged-in as an admin of the domain and specifies the domain name as contact
  name.

  Typical usage:
  contacts_service = gdata.contacts.service.ContactsService(
      email = 'admin@domain.com',
      password = '********',
      account_type = 'HOSTED',
      contact_list = 'domain.com',
      source = 'shared_contacts',
    )
  contacts_service.ProgrammaticLogin()
  contacts_manager = ContactsManager(contacts_service)
  contacts_manager.DeleteAllContacts()
  contacts_manager.ImportMsOutlookCsv(open('input.csv', 'rt'),
                                      open('output.csv', 'wb'))
  contacts_manager.ExportMsOutlookCsv(contacts_manager.GetAllContacts(),
                                      open('outlook.csv', 'wb'))
  """

  def __init__(self, contacts_service):
    """Creates a contact manager for the contact list of a domain or user.

    Args:
      contacts_service: The gdata.contacts.service.ContactsService instance to
        use to perform GData calls. Authentication should have been performed,
        typically by calling contacts_service.ProgrammaticLogin()
    """
    self.contacts_service = contacts_service

  def GetContactUrl(self, contact_short_id):
    """Retrieves the GData read-only URL of a contact from its short ID.

    Uses the /base projection.
    """
    return self.contacts_service.GetFeedUri(
        scheme='http', projection='base/%s' % contact_short_id)

  def GetContactsFeedUrl(self):
    """Retrieves the feed URL of the first READ_CHUNK_SIZE contacts."""
    feed_uri = self.contacts_service.GetFeedUri()
    query = gdata.contacts.service.ContactsQuery(feed_uri)
    query.max_results = READ_CHUNK_SIZE
    return query.ToUri()

  def GetAllContacts(self):
    """Retrieves all contacts in the contact list.

    Yields:
      gdata.contacts.ContactEntry objects.
    """
    feed_url = self.GetContactsFeedUrl()
    total_read = 0
    while True:
      Log('Retrieving contacts... (%d retrieved so far)' % total_read)
      feed = self.contacts_service.GetContactsFeed(feed_url)
      total_read += len(feed.entry)
      for entry in feed.entry:
        yield entry
      next_link = feed.GetNextLink()
      if next_link is None:
        Log('All contacts retrieved: %d total' % total_read)
        break
      feed_url = next_link.href

  def ImportMsOutlookCsv(self, input_csv_file, output_csv_file, dry_run=False):
    """Imports an MS Outlook contacts CSV file into the contact list.

    Contacts are batch-imported by chunks of BATCH_CHUNK_SIZE.

    Args:
      input_csv_file: The MS Outlook CSV file to import, as a readable stream.
      output_csv_file: The file where the added and updated contacts CSV
        entries, as a writable stream. Optional.
      dry_run: If set to True, reads the CSV file but does not actually import
        the contact entries. Useful to check the CSV file syntax.
    """
    outlook_serializer = OutlookSerializer()
    csv_reader = csv.DictReader(input_csv_file, delimiter=',')
    if output_csv_file:
      csv_writer = outlook_serializer.CreateCsvWriter(output_csv_file)
    else:
      csv_writer = None

    def WriteCsvRow(contact_entry):
      if csv_writer:
        fields = outlook_serializer.ContactEntryToFields(contact_entry)
        csv_writer.writerow(fields)

    ignored = [0]
    def CsvLineToOperation((index, fields)):
      """Maps a CSV line to an operation on a contact.

      Args:
        fields: The fields dictionary of a CSV line.

      Returns:
        An action tuple (action, entry), where action is taken from the "Action"
        field and entry the contact GData entry built from the CSV line.
      """
      action = fields.get('Action', DEFAULT_ACTION).lower()
      contact_id = fields.get('ID')
      contact_entry = outlook_serializer.FieldsToContactEntry(fields)

      if action not in ACTIONS:
        PrintContact(index, contact_entry, contact_id,
            ' Invalid action: %s - ignoring the entry' % action)
        ignored[0] += 1
        return None
      if action == ACTION_ADD and contact_id:
        PrintContact(index, contact_entry, contact_id,
            ' A contact to be added should not have an ID - ignoring the entry')
        ignored[0] += 1
        return None
      if action in (ACTION_UPDATE, ACTION_DELETE) and not contact_id:
        PrintContact(index, contact_entry, contact_id, ' A contact to be '
            'updated or deleted must have an ID - ignoring the entry')
        ignored[0] += 1
        return None

      PrintContact(index, contact_entry, contact_id, ' [%s]' % action)
      return (index, action, contact_id, contact_entry)

    operations_it = itertools.imap(CsvLineToOperation, enumerate(csv_reader))
    operations_it = itertools.ifilter(None, operations_it)

    total_stats = ImportStats()

    # Copies the IDs (read-only and edit) from an entry to another.
    # This is needed for updates and deletes.
    def CopyContactId(from_entry, to_entry):
      to_entry.id = from_entry.id
      to_entry.category = from_entry.category
      to_entry.link = from_entry.link

    for operations_chunk in Chunks(operations_it, BATCH_CHUNK_SIZE):
      query_feed = gdata.contacts.ContactsFeed()

      # First pass: we need the edit links for existing entries
      # - Update action: query
      # - Delete action: query
      #
      # Second pass: make modifications
      # - Add action: insert
      # - Update action: update
      # - Delete action: delete

      chunk_stats = ImportStats()

      # First pass preparation
      for (index, action, contact_id, contact_entry) in operations_chunk:
        if contact_id:
          url = self.GetContactUrl(contact_id)
          query_feed.AddQuery(url, batch_id_string=str(index))

      # First pass execution
      if not query_feed.entry:
        Log('Skipping query pass: nothing to update or delete')
        queried_results_by_index = {}
      else:
        Log('Querying %d contact(s)...' % len(query_feed.entry))
        queried_results_by_index = self._ExecuteBatch(query_feed)

      # Second pass preparation
      mutate_feed = gdata.contacts.ContactsFeed()
      for (index, action, contact_id, new_entry) in operations_chunk:
        queried_result = queried_results_by_index.get(index)
        if queried_result:
          queried_result.PrintResult(action, contact_id, new_entry)

        batch_id_string = str(index)

        # The entry is modified by side-effect by Batch feeds.
        # Copy it to avoid that.
        new_entry = copy.deepcopy(new_entry)

        if action == ACTION_ADD:
          chunk_stats.added_total += 1
          mutate_feed.AddInsert(entry=new_entry,
                                batch_id_string=batch_id_string)

        elif action == ACTION_UPDATE:
          chunk_stats.updated_total += 1
          if queried_result and queried_result.is_success:
            CopyContactId(queried_result.entry, new_entry)
            mutate_feed.AddUpdate(entry=new_entry,
                                  batch_id_string=batch_id_string)

        elif action == ACTION_DELETE:
          chunk_stats.deleted_total += 1
          if queried_result and queried_result.is_success:
            mutate_feed.AddDelete(queried_result.entry.GetEditLink().href,
                                  batch_id_string=batch_id_string)
          elif queried_result and queried_result.code == 404:
            chunk_stats.deleted_done += 1

      # Second pass execution
      if dry_run:
        Log('[Dry run] %d contact(s) would have been mutated' %
            len(mutate_feed.entry))
      elif not mutate_feed.entry:
        Log('Skipping mutate pass: nothing to mutate')
      else:
        Log('Mutating %d contact(s)...' % len(mutate_feed.entry))
        mutated_results_by_index = self._ExecuteBatch(mutate_feed)

        # Second pass results
        for (index, action, contact_id, new_entry) in operations_chunk:
          mutated_result = mutated_results_by_index.get(index)
          if mutated_result:
            details = None
            if mutated_result.is_success:
              if action == ACTION_ADD:
                contact_id = GetContactShortId(mutated_result.entry)
                details = 'added as: %s' % contact_id
                chunk_stats.added_done += 1
                WriteCsvRow(mutated_result.entry)
              elif action == ACTION_UPDATE:
                chunk_stats.updated_done += 1
                WriteCsvRow(mutated_result.entry)
              elif action == ACTION_DELETE:
                chunk_stats.deleted_done += 1
            mutated_result.PrintResult(action, contact_id, new_entry, details)

      # Print statistics
      Log('Contacts %s' % chunk_stats)

      # Update total statistics
      total_stats.Add(chunk_stats)

    # Print total statistics
    Log('### Total contacts %s - ignored: %s' % (total_stats, ignored[0]))

  def ExportMsOutlookCsv(self, contact_entries, csv_file):
    """Exports some contacts to a CSV file in MS Outlook format.

    Args:
      contact_entries: The contacts to export.
      csv_file: The MS Outlook CSV file to export to, as a writable stream.
    """
    outlook_serializer = OutlookSerializer()
    csv_writer = outlook_serializer.CreateCsvWriter(csv_file)
    csv_writer.writerows(itertools.imap(outlook_serializer.ContactEntryToFields,
                                        contact_entries))
    Log('### Exported.')

  def _ExecuteBatch(self, batch_feed):
    """Executes a batch feed.

    Args:
      batch_feed: The feed to execute.

    Returns:
      A dictionary mapping result batch indices (as integers) to the matching
      BatchResult objects.
    """
    batch_uri = self.contacts_service.GetFeedUri(projection='base/batch')
    result_feed = self.contacts_service.ExecuteBatch(batch_feed, batch_uri)
    results = map(self.BatchResult, result_feed.entry)
    results_by_index = dict((result.batch_index, result) for result in results)
    return results_by_index

  class BatchResult(object):
    def __init__(self, result_entry):
      self.batch_index = int(result_entry.batch_id.text)
      self.entry = result_entry
      self.status = result_entry.batch_status
      self.code = int(self.status.code)
      self.is_success = (self.code < 400)

    def PrintResult(self, action, contact_id, new_entry, more=None):
      outcome = self.is_success and 'OK' or 'Error'
      message = ' [%s] %s %s: %s' % (
          action, outcome, self.status.code, self.status.reason)
      if self.status.text:
        existing_entry = gdata.contacts.ContactEntryFromString(self.status.text)
        existing_id = GetContactShortId(existing_entry)
        message = '%s - existing ID: %s' % (message, existing_id)
      if more:
        message = '%s %s' % (message, more)
      PrintContact(self.batch_index, new_entry, contact_id, message)

  def DeleteAllContacts(self):
    """Empties the contact list. Asks for confirmation first."""
    confirmation = raw_input(
        'Do you really want to delete all contact(s) of %s? [y/N] ' %
            self.contacts_service.contact_list)
    if confirmation.lower() != 'y':
      return False

    feed_url = self.GetContactsFeedUrl()
    batch_uri = self.contacts_service.GetFeedUri(projection='full/batch')
    deleted_total = 0
    Log('### Deleting all contacts...')
    while True:

      # Retrieve a chunk of contacts
      Log('Retrieving %d contacts to delete...' % READ_CHUNK_SIZE)
      read_feed = self.contacts_service.GetContactsFeed(feed_url)
      if not read_feed.entry:
        break

      # Delete the contacts in batch, in smaller chunks
      for chunk in Chunks(read_feed.entry, BATCH_CHUNK_SIZE):
        Log('Deleting %d contacts... (%d deleted so far)' % (
            len(chunk), deleted_total))
        delete_feed = gdata.contacts.ContactsFeed()
        for contact_entry in chunk:
          delete_feed.AddDelete(contact_entry.GetEditLink().href)
        results = self.contacts_service.ExecuteBatch(delete_feed, batch_uri)
        for result in map(self.BatchResult, results.entry):
          if result.is_success:
            deleted_total += 1
          else:
            result.PrintResult('delete', GetContactShortId(result.entry), None)

    Log('All contacts deleted: %d total' % deleted_total)


class OutlookSerializer(object):

  """Converts MS Outlook contacts CSV rows from/to ContactEntry."""

  def __init__(self):
    """Builds a new Outlook to GData converter."""
    self.display_name_fields = (
        'First Name', 'Middle Name', 'Last Name', 'Suffix')

    self.email_addresses = (  # Field name, relation, is primary
        ('E-mail Address', gdata.contacts.REL_WORK, 'true'),
        ('E-mail 2 Address', gdata.contacts.REL_HOME, None),
        ('E-mail 3 Address', gdata.contacts.REL_OTHER, None),
      )

    self.postal_addresses = (  # Field name, relation
        ('Home Address', gdata.contacts.REL_HOME),
        ('Business Address', gdata.contacts.REL_WORK),
        ('Other Address', gdata.contacts.REL_OTHER),
      )

    self.primary_phone_numbers = (
        ('Business Fax', gdata.contacts.PHONE_WORK_FAX),
        ('Business Phone', gdata.contacts.PHONE_WORK),
        ('Business Phone 2', gdata.contacts.PHONE_WORK),
        ('Home Fax', gdata.contacts.PHONE_HOME_FAX),
        ('Home Phone', gdata.contacts.PHONE_HOME),
        ('Home Phone 2', gdata.contacts.PHONE_HOME),
        ('Other Phone', gdata.contacts.PHONE_OTHER),
        ('Mobile Phone', gdata.contacts.PHONE_MOBILE),
        ('Pager', gdata.contacts.PHONE_PAGER),
      )
    self.other_phone_numbers = (
        ("Assistant's Phone", gdata.contacts.PHONE_WORK),
        ('Callback', gdata.contacts.PHONE_OTHER),
        ('Car Phone', gdata.contacts.PHONE_CAR),
        ('Company Main Phone', gdata.contacts.PHONE_GENERAL),
        ('ISDN', gdata.contacts.PHONE_OTHER),
        ('Other Fax', gdata.contacts.PHONE_FAX),
        ('Primary Phone', gdata.contacts.PHONE_WORK),
        ('Radio Phone', gdata.contacts.PHONE_OTHER),
        ('TTY/TDD Phone', gdata.contacts.PHONE_OTHER),
        ('Telex', gdata.contacts.PHONE_OTHER),
      )
    self.phone_numbers = tuple(list(self.primary_phone_numbers) +
                               list(self.other_phone_numbers))

    export_fields = [
        'Action',
        'ID',
        'Name',
        'Company',
        'Job Title',
        'Notes',
      ]
    def AppendFields(fields):
      export_fields.extend(map(operator.itemgetter(0), fields))
    map(AppendFields, (self.email_addresses,
                       self.postal_addresses,
                       self.primary_phone_numbers))
    self.export_fields = tuple(export_fields)

  def FieldsToContactEntry(self, fields):
    """Converts a map of fields to values to a gdata.contacts.ContactEntry.

    Unknown fields are ignored.

    Args:
      fields: A dictionary mapping MS Outlook CSV field names to values.

    Returns:
      A gdata.contacts.ContactEntry instance equivalent to the provided fields.
    """
    contact_entry = gdata.contacts.ContactEntry()

    def GetField(name):
      value = fields.get(name) or ""
      return value.strip()

    name = GetField('Name')
    if not name:
      name = ' '.join(filter(None, map(GetField, self.display_name_fields)))
    contact_entry.title = atom.Title(text=name)

    notes = GetField('Notes')
    if notes:
      contact_entry.content = atom.Content(text=notes)

    company_name = GetField('Company')
    company_title = GetField('Job Title')
    if company_name or company_title:
      org_name = None
      if company_name:
        org_name = gdata.contacts.OrgName(text=company_name)
      org_title = None
      if company_title:
        org_title = gdata.contacts.OrgTitle(text=company_title)
      contact_entry.organization = gdata.contacts.Organization(
          org_name=org_name, org_title=org_title)

    for (field_name, rel, is_primary) in self.email_addresses:
      email_address = GetField(field_name)
      if email_address:
        contact_entry.email.append(gdata.contacts.Email(
            address=email_address, primary=is_primary, rel=rel))

    for (field_name, rel) in self.postal_addresses:
      postal_address = GetField(field_name)
      if postal_address:
        contact_entry.postal_address.append(gdata.contacts.PostalAddress(
            text=postal_address, rel=rel))

    for (field_name, rel) in self.phone_numbers:
      phone_number = GetField(field_name)
      if phone_number:
        contact_entry.phone_number.append(gdata.contacts.PhoneNumber(
            text=phone_number, rel=rel))

    return contact_entry

  def CreateCsvWriter(self, csv_file):
    """Creates a CSV writer the given file.

    Writes the CSV column names to the file.

    Args:
      csv_file: The file to write CSV entries to, as a writable stream.

    Returns:
      The created csv.DictWriter.
    """
    csv_writer = csv.DictWriter(csv_file, delimiter=',',
                                fieldnames=self.export_fields)
    csv_writer.writerow(dict(zip(self.export_fields, self.export_fields)))
    return csv_writer

  def ContactEntryToFields(self, contact_entry):
    """Converts a ContactsEntry to a CSV row dictionary.

    The CSV row columns are supposed to be self.export_fields.

    Args:
      contact_entry: The gdata.contacts.ContactEntry instance to convert.

    Returns:
      A dictionary mapping MS Outlook CSV field names to values.
      The dictionary keys are a subset of ContactEntryToFields().
    """
    fields = {}
    def AddField(field_name, obj, attribute_name):
      """Populates a CSV field from an attribute of the given object.

      Does nothing if the object is None.

      Args:
        field_name: The name of the CSV field to populate.
        obj: The object to read an attribute of. Can be None.
        attribute_name: The name of the attribute of 'obj' to retrieve.
      """
      if obj:
        fields[field_name] = getattr(obj, attribute_name)

    fields['Action'] = ACTION_UPDATE
    fields['ID'] = GetContactShortId(contact_entry)

    AddField('Name', contact_entry.title, 'text')

    if contact_entry.organization:
      AddField('Company', contact_entry.organization.org_name, 'text')
      AddField('Job Title', contact_entry.organization.org_title, 'text')

    AddField('Notes', contact_entry.content, 'text')

    email_addresses = {}
    for email in contact_entry.email:
      email_addresses.setdefault(email.rel, email.address)
    for (field_name, rel, _) in self.email_addresses:
      fields[field_name] = email_addresses.get(rel, '')

    postal_addresses = {}
    for postal_address in contact_entry.postal_address:
      postal_addresses.setdefault(postal_address.rel, postal_address.text)
    for (field_name, rel) in self.postal_addresses:
      fields[field_name] = postal_addresses.get(rel, '')

    phone_numbers = {}
    for phone_number in contact_entry.phone_number:
      phone_numbers.setdefault(phone_number.rel, phone_number.text)
    for (field_name, rel) in self.primary_phone_numbers:
      fields[field_name] = phone_numbers.get(rel, '')

    return fields


def main():
  usage = """\
shared_contacts.py --admin=EMAIL [--clear] [--import=FILE [--output=FILE]]
  [--export=FILE]
If you specify several commands at the same time, they are executed in in the
following order: --clear, --import, --export regardless of the order of the
parameters in the command line."""
  parser = optparse.OptionParser(usage=usage)
  parser.add_option('-a', '--admin', default='', metavar='EMAIL',
      help="email address of an admin of the domain")
  parser.add_option('-p', '--password', default=None, metavar='PASSWORD',
      help="password of the --admin account")
  parser.add_option('-i', '--import', default=None, metavar='FILE',
      dest='import_csv', help="imports an MS Outlook CSV file, before export "
          "if --export is specified, after clearing if --clear is specified")
  parser.add_option('-o', '--output', default=None, metavar='FILE',
      dest='output_csv', help="output file for --import; will contain the "
          "added and updated contacts in the same format as --export")
  parser.add_option('--dry_run', action='store_true',
      help="does not authenticate and import contacts for real")
  parser.add_option('-e', '--export', default=None, metavar='FILE',
      dest='export_csv', help="exports all shared contacts of the domain as "
          "CSV, after clearing or import took place if --clear or --import is "
          "specified")
  parser.add_option('--clear', action='store_true',
      help="deletes all contacts; executed before --import and --export "
           "if any of these flags is specified too")
  (options, args) = parser.parse_args()
  if args:
    parser.print_help()
    parser.exit(msg='\nUnexpected arguments: %s' % ' '.join(args))

  admin_email = options.admin
  admin_password = options.password
  import_csv_file_name = options.import_csv
  output_csv_file_name = options.output_csv
  export_csv_file_name = options.export_csv
  clear = options.clear
  dry_run = options.dry_run

  if not filter(None, (import_csv_file_name, export_csv_file_name, clear)):
    parser.print_help()
    parser.exit(msg='\nNothing to do: specify --import, --export, or --clear')
  if output_csv_file_name and not import_csv_file_name:
    parser.print_help()
    parser.exit(msg='\n--output can be set only with --import')

  # Retrieve the domain from the admin email address
  if not admin_email:
    parser.error('Please set the --admin command-line option')
  domain_index = admin_email.find('@')
  if domain_index < 0:
    parser.error('Invalid admin email address: %s\n' % admin_email)
  domain = admin_email[domain_index+1:]

  # Check import_csv_file_name
  if import_csv_file_name:
    try:
      import_csv_file = open(import_csv_file_name, 'rt')
      Log('Outlook CSV file to import: %s' % import_csv_file_name)
    except IOError, e:
      parser.error('Unable to open %s\n%s\nPlease set the --import command-line'
          ' option to a readable file.' % (import_csv_file_name, e))

  def OpenOutputCsv(file_name, option_name, description):
    if file_name:
      try:
        csv_file = open(file_name, 'wb')
        Log('%s as CSV to: %s' % (description, file_name))
        return csv_file
      except IOError, e:
        parser.error('Unable to open %s\n%s\nPlease set the --%s command-line'
            ' option to a writable file.' % (file_name, option_name, e))
    else:
      return None

  output_csv_file = OpenOutputCsv(output_csv_file_name,
                                  'output', 'Save import output')
  export_csv_file = OpenOutputCsv(export_csv_file_name, 'export', 'Export')

  Log('Domain: %s' % domain)
  Log('Administrator: %s' % admin_email)
  if dry_run:
    Log('Dry mode enabled')

  # Ask for the admin password
  if admin_password is None:
    admin_password = getpass.getpass('Password of %s: ' % admin_email)
  else:
    Log('Using password passed to --password')

  # Construct the service and authenticate
  contacts_service = gdata.contacts.service.ContactsService(
      email = admin_email,
      password = admin_password,
      account_type = 'HOSTED',
      contact_list = domain,
      source = 'shared_contacts',
      additional_headers = {GDATA_VER_HEADER: 1}
    )
  contacts_service.ProgrammaticLogin()
  contacts_manager = ContactsManager(contacts_service)

  if clear:
    if dry_run:
      Log('--clear: ignored in dry mode')
    else:
      contacts_manager.DeleteAllContacts()

  if import_csv_file_name:
    Log('### Importing contacts CSV file: %s' % import_csv_file_name)
    contacts_manager.ImportMsOutlookCsv(import_csv_file, output_csv_file,
                                        dry_run=dry_run)
    import_csv_file.close()

  if export_csv_file_name:
    if dry_run:
      Log('--export: ignored in dry mode')
    else:
      Log('### Exporting contacts to CSV file: %s' % export_csv_file_name)
      contact_entries = contacts_manager.GetAllContacts()
      contacts_manager.ExportMsOutlookCsv(contact_entries, export_csv_file)
      export_csv_file.close()


if __name__ == '__main__':
  main()
