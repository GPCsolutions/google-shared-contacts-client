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
import codecs
import cStringIO
import pprint
import getpass
import itertools
import operator
import optparse
import sys
import atom
import gdata.data
import gdata.contacts.client
import gdata.contacts.data

# Maximum number of operations in batch feeds.
BATCH_CHUNK_SIZE = 100

# Number of contacts to retrieve at once in ContactsManager.GetAllContacts()
READ_CHUNK_SIZE = 500

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
    contact_entry: The gdata.contacts.data.ContactEntry instance to print.
    more: Additional text to append to the printed line.
  """
  primary_email = None
  display_name = None
  if contact_entry:
    for email in contact_entry.email:
      if email.primary and email.primary == 'true':
        primary_email = email.address
        break
    display_name = contact_entry.name.full_name.text
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
  """Provides high-level operations for a contacts/profiles list.

  Services:
  - importing contacts/profiles from an MS Outlook CSV file
  - exporting contacts/profiles to an MS Outlook CSV file
  - deleting all contacts

  The contact list is typically a domain contact list, that is the user is
  logged-in as an admin of the domain and specifies the domain name as contact
  name.

  Typical usage:
  contacts_client = gdata.contacts.client.ContactsService(
      email = 'admin@domain.com',
      password = '********',
      account_type = 'HOSTED',
      contact_list = 'domain.com',
      source = 'shared_contacts',
    )
  contacts_client.ProgrammaticLogin()
  contacts_manager = ContactsManager(contacts_client)
  contacts_manager.DeleteAllContacts()
  contacts_manager.ImportMsOutlookCsv(open('input.csv', 'rt'),
                                      open('output.csv', 'wb'))
  contacts_manager.ExportMsOutlookCsv(contacts_manager.GetAllContacts(),
                                      open('outlook.csv', 'wb'))
  """

  def __init__(self, contacts_client, domain):
    """Creates a contact manager for the contact/profile list of a domain or user.

    Args:
      contacts_client: The gdata.contacts.client.ContactsService instance to
        use to perform GData calls. Authentication should have been performed,
        typically by calling contacts_client.ProgrammaticLogin()
      domain: the domain for the shared contacts list or profiles
    """
    self.contacts_client = contacts_client
    self.domain = domain

  def GetContactUrl(self, contact_short_id):
    """Retrieves the GData read-only URL of a contact from its short ID.

    Uses the /full projection.
    """
    return self.contacts_client.GetFeedUri(
        contact_list=self.domain, scheme='http', projection='full/%s' % contact_short_id)
    
  def GetAllContacts(self):
    """Retrieves all contacts in the contact list.

    Yields:
      gdata.contacts.data.ContactEntry objects.
    """
    feed_url = self.contacts_client.GetFeedUri(contact_list=self.domain, projection='full')
    total_read = 0
    while True:
      Log('Retrieving contacts... (%d retrieved so far)' % total_read)      
      feed = self.contacts_client.get_feed(uri=feed_url,
                                           auth_token=None,
                                           desired_class=gdata.contacts.data.ContactsFeed)
      total_read += len(feed.entry)
      for entry in feed.entry:
        yield entry
      next_link = feed.GetNextLink()
      if next_link is None:
        Log('All contacts retrieved: %d total' % total_read)
        break
      feed_url = next_link.href

  def GetProfileUrl(self, profile_short_id):
    """Retrieves the GData read-only URL of a profile from its short ID.

    Uses the /full projection.
    """      
    return self.contacts_client.GetFeedUri(
        kind='profiles', contact_list=self.domain,
        scheme='http', projection='full/%s' % profile_short_id)  

  def GetAllProfiles(self):
    """Retrieves all profiles in the domain.

    Yields:
      gdata.contacts.data.ProfileEntry objects.
    """
    feed_url = self.contacts_client.GetFeedUri(kind='profiles',
                                               contact_list=self.domain,
                                               projection='full')
    total_read = 0
    while True:
      Log('Retrieving profiles... (%d retrieved so far)' % total_read)
      feed = self.contacts_client.get_feed(feed_url, auth_token=None,
                                           desired_class=gdata.contacts.data.ProfilesFeed)
      total_read += len(feed.entry)
      for entry in feed.entry:
        yield entry
      next_link = feed.GetNextLink()
      if next_link is None:
        Log('All profiles retrieved: %d total' % total_read)
        break
      feed_url = next_link.href
      
  def GetProfile(self,profile_short_id):
    """Gets a single profile from its short ID.

    Uses the /full projection.
    """        
    uri = self.GetProfileUrl(profile_short_id)      
    return self.contacts_client.Get(uri, desired_class=gdata.contacts.data.ProfileEntry)

  def ImportMsOutlookCsv(self, import_csv_file_name, output_csv_file, dry_run=False):
    """Imports an MS Outlook contacts/profiles CSV file into the contact list.

    Contacts are batch-imported by chunks of BATCH_CHUNK_SIZE.

    Args:
      import_csv_file_name: The MS Outlook CSV file name to import, as a readable stream.
      output_csv_file: The file where the added and updated contacts/profiles
        CSV entries, as a writable stream. Optional.
      dry_run: If set to True, reads the CSV file but does not actually import
        the contact entries. Useful to check the CSV file syntax.
    """
    outlook_serializer = OutlookSerializer()
      
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
      """Maps a CSV line to an operation on a contact/profile.

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
            ' (Invalid action: %s - ignoring the entry)' % action)
        ignored[0] += 1
        return None
      
      if action == ACTION_ADD and contact_id:
        PrintContact(index, contact_entry, contact_id,
            ' (A contact to be added should not have an ID or Profiles cannot be added - ignoring the entry)')
        ignored[0] += 1
        return None
      
      if action == ACTION_ADD and not contact_id:
        # if email address is in the domain the Contact could be a Profile
        for email in contact_entry.email:
          domain_str_index = email.address.find("@"+self.domain)          
          if domain_str_index > 0:
            # Email is part of the domain: a user or a group
            profile_short_id = email.address[:domain_str_index]
            tempProfileEntry = None
            try:
              # Testing if it is a Profile
              tempProfileEntry = self.GetProfile(profile_short_id)
            except gdata.client.RequestError, detail:
              # Skipping any errors and use Shared Contacts instead.
              pass
            # Checking if the Profiles API should be use for domain users            
            if tempProfileEntry:
               # Changing Shared Contact to a Profile Update Action
               contact_id = profile_short_id
               action = ACTION_UPDATE
               PrintContact(index, contact_entry, contact_id, ' [update] (using Profiles)')
               return (index, action, contact_id, contact_entry)
            
      if action in (ACTION_UPDATE, ACTION_DELETE) and not contact_id:
        PrintContact(index, contact_entry, contact_id, ' A contact to be '
            ' (update or delete must have an ID - ignoring the entry)')
        ignored[0] += 1
        return None

      PrintContact(index, contact_entry, contact_id, ' [%s]' % action)
      return (index, action, contact_id, contact_entry)
    
    # Finding the correct encoding for the file
    csv_reader = None
    all_encoding = ["utf-8", "iso-8859-1", "iso-8859-2", 'iso-8859-15', 'iso-8859-3', "us-ascii", 'windows-1250', 'windows-1252', 'windows-1254', 'ibm861']
    encoding_index = 0
    print "Detecting encoding of the CSV file..."
    while csv_reader == None:  
      next_encoding = all_encoding[encoding_index]
      print "Trying %s" % (next_encoding)
      input_csv_file = open(import_csv_file_name, 'rt')
      csv_reader = UnicodeDictReader(input_csv_file, delimiter=',', encoding=next_encoding)
      try:
        for line in enumerate(csv_reader):
            # Do nothing, just reading the whole file
            encoding_index = encoding_index
      except UnicodeDecodeError:
        csv_reader = None
        input_csv_file.close()
        encoding_index = encoding_index + 1
    
    print "Correct encoding of the file is %s" % (next_encoding)
    input_csv_file.close()
    input_csv_file = open(import_csv_file_name, 'rt')
    csv_reader = UnicodeDictReader(input_csv_file, delimiter=',', encoding=next_encoding)
    
    operations_it = itertools.imap(CsvLineToOperation, enumerate(csv_reader))
    operations_it = itertools.ifilter(None, operations_it)

    total_stats = ImportStats()

    # Copies the IDs (read-only and edit) from an entry to another.
    # This is needed for updates and deletes.
    def CopyContactId(from_entry, to_entry):
      to_entry.id = from_entry.id
      to_entry.category = from_entry.category
      to_entry.link = from_entry.link      
      to_entry.etag = from_entry.etag

    for operations_chunk in Chunks(operations_it, BATCH_CHUNK_SIZE):
      query_feed = gdata.data.BatchFeed()
      query_feed_profiles = gdata.data.BatchFeed()

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
          url_contact = self.GetContactUrl(contact_id)
          query_feed.AddQuery(url_contact, batch_id_string=str(index))
          url_profile = self.GetProfileUrl(contact_id)
          query_feed_profiles.AddQuery(url_profile, batch_id_string=str(index))
      
      # First pass execution
      if not query_feed.entry:
        Log('Skipping query pass: nothing to update or delete')
        queried_results_by_index = {}
        queried_results_by_index_profiles = {}
      else:
        Log('Querying %d contacts/profiles(s)...' % len(query_feed_profiles.entry))
        queried_results_by_index = self._ExecuteBatch(query_feed)
        queried_results_by_index_profiles = self._ExecuteBatchProfile(query_feed_profiles)
      
      # Second pass preparation
      mutate_feed = gdata.data.BatchFeed()
      mutate_feed_profiles = gdata.data.BatchFeed()
      for (index, action, contact_id, new_entry) in operations_chunk:
        # Contact
        queried_result_contact = queried_results_by_index.get(index)
        # Profile
        queried_result_profiles = queried_results_by_index_profiles.get(index)                                
        # if is a Contact then is not a Profile
        if queried_result_contact and not queried_result_profiles.is_success:
          queried_result_contact.PrintResult(action, contact_id, new_entry, '(Shared Contact)')        
        # if is a Profile then is not a Contact:
        if queried_result_profiles and not queried_result_contact.is_success:
          queried_result_profiles.PrintResult(action, contact_id, new_entry, '(Profile)')

        batch_id_string = str(index)

        # The entry is modified by side-effect by Batch feeds.
        # Copy it to avoid that.
        new_entry = copy.deepcopy(new_entry)

        # ADD is only supported by Contacts (Not supported by Profiles)
        if action == ACTION_ADD:          
          chunk_stats.added_total += 1
          mutate_feed.AddInsert(entry=new_entry,
                                batch_id_string=batch_id_string)

        elif action == ACTION_UPDATE:
          chunk_stats.updated_total += 1
          # Update Contact
          if queried_result_contact and queried_result_contact.is_success:
            CopyContactId(queried_result_contact.entry, new_entry)
            mutate_feed.AddUpdate(entry=new_entry,
                                  batch_id_string=batch_id_string)
          # Update Profile
          if queried_result_profiles and queried_result_profiles.is_success:
            CopyContactId(queried_result_profiles.entry, new_entry)
            mutate_feed_profiles.AddUpdate(entry=new_entry,
                                  batch_id_string=batch_id_string)

        # DELETE is only supported by Contacts (Not supported by Profiles)
        elif action == ACTION_DELETE:          
          if queried_result_profiles and queried_result_profiles.is_success:
            queried_result_profiles.PrintResult(action, contact_id, new_entry, '(A Profile cannot be deleted - ignoring the entry)')                        
            ignored[0] += 1
          else:
            chunk_stats.deleted_total += 1
            if queried_result_contact and queried_result_contact.is_success:              
              mutate_feed.AddDelete(queried_result_contact.entry.GetEditLink().href,
                                    queried_result_contact.entry,
                                  batch_id_string=batch_id_string)

      # Second pass execution
      if dry_run:
        Log('[Dry run] %d contact(s) would have been mutated' %
            len(mutate_feed.entry))
        Log('[Dry run] %d profiles(s) would have been mutated' %
            len(mutate_feed_profiles.entry))      
      else:
        # Second pass results Contacts
        if not mutate_feed.entry:
          Log('Skipping Contacts mutate pass: no Contacts to mutate')
        else:                  
          Log('Mutating %d contact(s)...' % len(mutate_feed.entry))
          mutated_results_by_index = self._ExecuteBatch(mutate_feed)
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
        # Second pass results Profiles
        if not mutate_feed_profiles.entry:
          Log('Skipping Profiles mutate pass: no Profiles to mutate')
        else:
          Log('Mutating %d profiles(s)...' % len(mutate_feed_profiles.entry))
          mutated_results_by_index_profiles = self._ExecuteBatchProfile(mutate_feed_profiles)        
          for (index, action, contact_id, new_entry) in operations_chunk:
            mutated_result_profiles = mutated_results_by_index_profiles.get(index)
            if mutated_result_profiles:
              details = None
              if mutated_result_profiles.is_success:
                chunk_stats.updated_done += 1
                WriteCsvRow(mutated_result_profiles.entry)             
              mutated_result_profiles.PrintResult(action, contact_id, new_entry, details)

      # Print statistics
      Log('Contacts/Profiles %s' % chunk_stats)

      # Update total statistics
      total_stats.Add(chunk_stats)

    input_csv_file.close()
    # Print total statistics
    Log('### Total contacts/profiles %s - ignored: %s' % (total_stats, ignored[0]))

  def ExportMsOutlookCsv(self, contact_entries, profile_entries, csv_file):
    """Exports contacts/profiles to a CSV file in MS Outlook format.

    Args:
      contact_entries: The contacts to export.
      csv_file: The MS Outlook CSV file to export to, as a writable stream.
    """
    outlook_serializer = OutlookSerializer()    
    csv_writer = outlook_serializer.CreateCsvWriter(csv_file)
    csv_writer.writerows(itertools.imap(outlook_serializer.ContactEntryToFields,
                                        contact_entries))
    csv_writer.writerows(itertools.imap(outlook_serializer.ContactEntryToFields,
                                        profile_entries))
    Log('### Exported.')

  def _ExecuteBatch(self, batch_feed):
    """Executes a batch contacts feed.

    Args:
      batch_feed: The feed to execute.

    Returns:
      A dictionary mapping result batch indices (as integers) to the matching
      BatchResult objects.
    """
    batch_uri = self.contacts_client.GetFeedUri(contact_list=self.domain,
                                                projection='full/batch')    
    
    result_feed = self.contacts_client.ExecuteBatch(batch_feed, 
                                                     batch_uri,
                                                     desired_class=gdata.contacts.data.ContactsFeed)
    
    results = map(self.BatchResult, result_feed.entry)
    results_by_index = dict((result.batch_index, result) for result in results)
      
    return results_by_index
    
  def _ExecuteBatchProfile(self, batch_feed):
    """Executes a batch profiles feed.

    Args:
      batch_feed: The feed to execute.

    Returns:
      A dictionary mapping result batch indices (as integers) to the matching
      BatchResult objects.
    """
    
    batch_uri = self.contacts_client.GetFeedUri(kind='profiles',
                                                contact_list=self.domain,
                                                projection='full/batch')    
    
    result_feed = self.contacts_client.ExecuteBatch(batch_feed, 
                                                     batch_uri,
                                                     desired_class=gdata.contacts.data.ProfilesFeed)
    
    results = map(self.BatchResult, result_feed.entry)
    results_by_index = dict((result.batch_index, result) for result in results)
    return results_by_index
    
  class BatchResult(object):
    def __init__(self, result_entry):
      if(result_entry.batch_id == None):
        self.batch_index = 99   
        self.code = 500
        self.status = None
      else:
        self.batch_index = int(result_entry.batch_id.text)
        self.status = result_entry.batch_status
        self.code = int(self.status.code)
      self.entry = result_entry
      self.is_success = (self.code < 400)

    def PrintResult(self, action, contact_id, new_entry, more=None):
      outcome = self.is_success and 'OK' or 'Error'   
      if(self.status != None):
        message = ' [%s] %s %i: %s' % (
            action, outcome, self.code, self.status.reason)
        if self.status.text:
          Log('Error: %s' % self.status.text)
          existing_id = GetContactShortId(existing_entry)
          message = '%s - existing ID: %s' % (message, existing_id)
        if more:
          message = '%s %s' % (message, more)
        PrintContact(self.batch_index, new_entry, contact_id, message)
      else:  
        Log('  ...)  Error Batch Interrupted')

  def DeleteAllContacts(self):
    """Empties the contact list. Asks for confirmation first."""
    confirmation = raw_input(
        'Do you really want to delete all Shared Contact(s) of %s? [y/N] ' %
            self.contacts_client.contact_list)
    if confirmation.lower() != 'y':
      return False

    feed_url = self.contacts_client.GetFeedUri(contact_list=self.domain)
    batch_uri = self.contacts_client.GetFeedUri(contact_list=self.domain,projection='full/batch')
    deleted_total = 0
    Log('### Deleting all Shared Contacts...')
    while True:
      # Retrieve a chunk of contacts
      Log('Retrieving %d contacts to delete...' % READ_CHUNK_SIZE)
      read_feed = self.contacts_client.get_feed(uri=feed_url,
        auth_token=None, desired_class=gdata.contacts.data.ContactsFeed)
      if not read_feed.entry:
        break
      # Delete the contacts in batch, in smaller chunks
      for chunk in Chunks(read_feed.entry, BATCH_CHUNK_SIZE):        
        delete_feed = gdata.contacts.data.ContactsFeed()
        for contact_entry in chunk:
          delete_feed.add_delete(contact_entry.GetEditLink().href, contact_entry)
        Log('Deleting %d contacts... (%d deleted so far)' % (
            len(delete_feed.entry), deleted_total))        
        results = self.contacts_client.ExecuteBatch(delete_feed, batch_uri)
        for result in map(self.BatchResult, results.entry):
          if result.is_success:
            deleted_total += 1
          else:
            result.PrintResult('delete', GetContactShortId(result.entry), None)
        
    Log('All Shared Contacts deleted: %d total' % deleted_total)

class UTF8Recoder:
    """
    Iterator that reads an encoded stream and reencodes the input to UTF-8
    """
    def __init__(self, f, encoding):
        self.reader = codecs.getreader(encoding)(f)

    def __iter__(self):
        return self

    def next(self):
        return self.reader.next().encode("utf-8")

class UnicodeDictReader:
    """
    A CSV reader which will iterate over lines in the CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, delimiter=',', dialect=csv.excel, encoding="utf-8"):
        f = UTF8Recoder(f, encoding)
        self.reader = csv.DictReader(f, delimiter=delimiter, dialect=dialect)

    def next(self):
        row = self.reader.next()
        for key in row:
            if row[key] != None:
              try:
                row[key] = row[key].decode("utf-8")
              # Special case, sometimes the content gets reqd as a list
              except AttributeError:
                  newList = []
                  for item in row[key]:
                      newList.append(item.decode("utf-8"))
                  row[key] = newList
            else:
              row[key] = ''
        return row

    def __iter__(self):
        return self

class UnicodeDictWriter:
    """
    A CSV writer which will write rows to CSV file "f",
    which is encoded in the given encoding.
    """

    def __init__(self, f, fieldnames, delimiter=',', dialect=csv.excel, encoding="utf-8"):
        # Redirect output to a queue
        self.queue = cStringIO.StringIO()
        self.writer = csv.DictWriter(self.queue, fieldnames, delimiter=delimiter)
        self.stream = f
        self.encoder = codecs.getincrementalencoder(encoding)()

    def writerow(self, row):
        rowEncodedCopy = {}
        for key in row:
            if row[key] != None:
              rowEncodedCopy[key] = row[key].encode("utf-8", "ignore")
            else:
              rowEncodedCopy[key] = row[key]
              
        self.writer.writerow(rowEncodedCopy)
        # Fetch UTF-8 output from the queue ...
        data = self.queue.getvalue()
        data = data.decode("utf-8")
        # ... and reencode it into the target encoding
        data = self.encoder.encode(data)
        # write to the target stream
        self.stream.write(data)
        # empty queue
        self.queue.truncate(0)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


class OutlookSerializer(object):

  """Converts MS Outlook contacts/profiles CSV rows from/to ContactEntry/ProfileEntry."""

  def __init__(self):
    """Builds a new Outlook to GData converter."""
    self.display_name_fields = (
        'First Name', 'Middle Name', 'Last Name', 'Suffix')

    self.email_addresses = (  # Field name, relation, is primary, priority
        ('E-mail Address', gdata.data.WORK_REL, 'true', 0),
        ('E-mail 2 Address', gdata.data.HOME_REL, None, 0),
        ('E-mail 3 Address', gdata.data.OTHER_REL, None, 0),
        ('E-mail 4 Address', gdata.data.WORK_REL, None, 1),
        ('E-mail 5 Address', gdata.data.HOME_REL, None, 1),
        ('E-mail 6 Address', gdata.data.OTHER_REL, None, 1),
        ('E-mail 7 Address', gdata.data.WORK_REL, None, 2),
        ('E-mail 8 Address', gdata.data.HOME_REL, None, 2),
        ('E-mail 9 Address', gdata.data.OTHER_REL, None, 2),        
        ('E-mail 10 Address', gdata.data.WORK_REL, None, 3),
        ('E-mail 11 Address', gdata.data.HOME_REL, None, 3),
        ('E-mail 12 Address', gdata.data.OTHER_REL, None, 3),
        ('E-mail 13 Address', gdata.data.WORK_REL, None, 4),
        ('E-mail 14 Address', gdata.data.HOME_REL, None, 4),
        ('E-mail 15 Address', gdata.data.OTHER_REL, None, 4),        
        ('E-mail 16 Address', gdata.data.WORK_REL, None, 5),
        ('E-mail 17 Address', gdata.data.HOME_REL, None, 5),
        ('E-mail 18 Address', gdata.data.OTHER_REL, None, 5),
        ('E-mail 19 Address', gdata.data.WORK_REL, None, 6),
        ('E-mail 20 Address', gdata.data.HOME_REL, None, 6),
        ('E-mail 21 Address', gdata.data.OTHER_REL, None, 6),
        ('E-mail 22 Address', gdata.data.WORK_REL, None, 7),
        ('E-mail 23 Address', gdata.data.HOME_REL, None, 7),
        ('E-mail 24 Address', gdata.data.OTHER_REL, None, 7),                
        ('E-mail 25 Address', gdata.data.WORK_REL, None, 8),
        ('E-mail 26 Address', gdata.data.HOME_REL, None, 8),
        ('E-mail 27 Address', gdata.data.OTHER_REL, None, 8),
        ('E-mail 28 Address', gdata.data.WORK_REL, None, 9),
        ('E-mail 29 Address', gdata.data.HOME_REL, None, 9),
        ('E-mail 30 Address', gdata.data.OTHER_REL, None, 9),
        ('E-mail 31 Address', gdata.data.WORK_REL, None, 10),
        ('E-mail 32 Address', gdata.data.HOME_REL, None, 10),
        ('E-mail 33 Address', gdata.data.OTHER_REL, None, 10),
        ('E-mail 34 Address', gdata.data.WORK_REL, None, 11),
        ('E-mail 35 Address', gdata.data.HOME_REL, None, 11),
        ('E-mail 36 Address', gdata.data.OTHER_REL, None, 11),
        ('E-mail 37 Address', gdata.data.WORK_REL, None, 12),
        ('E-mail 38 Address', gdata.data.HOME_REL, None, 12),
        ('E-mail 39 Address', gdata.data.OTHER_REL, None, 12),
        ('E-mail 40 Address', gdata.data.WORK_REL, None, 13),
        ('E-mail 41 Address', gdata.data.HOME_REL, None, 13),
        ('E-mail 42 Address', gdata.data.OTHER_REL, None, 13),
        ('E-mail 43 Address', gdata.data.WORK_REL, None, 14),
        ('E-mail 44 Address', gdata.data.HOME_REL, None, 14),
        ('E-mail 45 Address', gdata.data.OTHER_REL, None, 14),
        ('E-mail 46 Address', gdata.data.WORK_REL, None, 15),
        ('E-mail 47 Address', gdata.data.HOME_REL, None, 15),
        ('E-mail 48 Address', gdata.data.OTHER_REL, None, 15),
        ('E-mail 49 Address', gdata.data.WORK_REL, None, 16),
        ('E-mail 50 Address', gdata.data.HOME_REL, None, 16),
        ('E-mail 51 Address', gdata.data.OTHER_REL, None, 16),
        ('E-mail 52 Address', gdata.data.WORK_REL, None, 17),
        ('E-mail 53 Address', gdata.data.HOME_REL, None, 17),
        ('E-mail 54 Address', gdata.data.OTHER_REL, None, 17),
        ('E-mail 55 Address', gdata.data.WORK_REL, None, 18),
        ('E-mail 56 Address', gdata.data.HOME_REL, None, 18),
        ('E-mail 57 Address', gdata.data.OTHER_REL, None, 18),
        ('E-mail 58 Address', gdata.data.WORK_REL, None, 19),
        ('E-mail 59 Address', gdata.data.HOME_REL, None, 19),
        ('E-mail 60 Address', gdata.data.OTHER_REL, None, 19),
        ('E-mail 61 Address', gdata.data.WORK_REL, None, 20),
        ('E-mail 62 Address', gdata.data.HOME_REL, None, 20),
        ('E-mail 63 Address', gdata.data.OTHER_REL, None, 20),
        ('E-mail 64 Address', gdata.data.WORK_REL, None, 21),
        ('E-mail 65 Address', gdata.data.HOME_REL, None, 21),
        ('E-mail 66 Address', gdata.data.OTHER_REL, None, 21),
        ('E-mail 67 Address', gdata.data.WORK_REL, None, 22),
        ('E-mail 68 Address', gdata.data.HOME_REL, None, 22),
        ('E-mail 69 Address', gdata.data.OTHER_REL, None, 22),
      )

    self.postal_addresses = (  # Field name, relation
        ('Home Address', gdata.data.HOME_REL),
        ('Business Address', gdata.data.WORK_REL),
        ('Other Address', gdata.data.OTHER_REL),
      )

    self.primary_phone_numbers = ( # Field name, relation, priority
        ('Business Fax', gdata.data.WORK_FAX_REL, 0),
        ('Business Phone', gdata.data.WORK_REL, 0),
        ('Business Phone 2', gdata.data.WORK_REL, 1),
        ('Home Fax', gdata.data.HOME_FAX_REL, 0),
        ('Home Phone', gdata.data.HOME_REL, 0),
        ('Home Phone 2', gdata.data.HOME_REL, 1),
        ('Other Phone', gdata.data.OTHER_REL, 0 ),
        ('Mobile Phone', gdata.data.MOBILE_REL, 0),
        ('Pager', gdata.data.PAGER_REL, 0),
      )
    self.other_phone_numbers = ( # Field name, relation, priority
        ("Assistant's Phone", gdata.data.WORK_REL, 2),
        ('Callback', gdata.data.OTHER_REL, 1),
        ('Car Phone', gdata.data.CAR_REL, 0),
        ('Company Main Phone', gdata.data.COMPANY_MAIN_REL, 0),
        ('ISDN', gdata.data.OTHER_REL, 2),
        ('Other Fax', gdata.data.FAX_REL, 0),
        ('Primary Phone', gdata.data.WORK_REL, 3),
        ('Radio Phone', gdata.data.OTHER_REL, 3),
        ('TTY/TDD Phone', gdata.data.OTHER_REL, 4),
        ('Telex', gdata.data.OTHER_REL, 5),
      )
    self.phone_numbers = tuple(list(self.primary_phone_numbers) +
                               list(self.other_phone_numbers))
    
    self.websites = (  # Field name, relation
        ('Website Home-Page', 'home-page'),
        ('Website Blog', 'blog'),
        ('Website Profile', 'profile'),
        ('Website Home', 'home'),
        ('Website Work', 'work'),        
        ('Website Other', 'other'),
        ('Website FTP', 'ftp'),
      )

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
    map(AppendFields, (self.primary_phone_numbers,
                       self.postal_addresses,
                       self.email_addresses,
                       self.websites))
    self.export_fields = tuple(export_fields)

  def FieldsToContactEntry(self, fields):
    """Converts a map of fields to values to a gdata.contacts.data.ContactEntry.

    Unknown fields are ignored.

    Args:
      fields: A dictionary mapping MS Outlook CSV field names to values.

    Returns:
      A gdata.contacts.data.ContactEntry instance equivalent to the provided fields.
    """
    contact_entry = gdata.contacts.data.ContactEntry()

    def GetField(name):
      value = fields.get(name) or ""
      return value.strip()

    name = GetField('Name')
    if not name:
      name = ' '.join(filter(None, map(GetField, self.display_name_fields)))            
    contact_entry.name = gdata.data.Name(full_name=gdata.data.FullName(text=name))

    notes = GetField('Notes')
    if notes:
      contact_entry.content = atom.data.Content(text=notes)

    company_name = GetField('Company')
    company_title = GetField('Job Title')    
    if company_name or company_title:
      org_name = None
      if company_name:
        org_name = gdata.data.OrgName(text=company_name)
      org_title = None
      if company_title:
        org_title = gdata.data.OrgTitle(text=company_title)
      contact_entry.organization = gdata.data.Organization(
          name=org_name, title=org_title)
      contact_entry.organization.rel = gdata.data.OTHER_REL

    for (field_name, rel, is_primary, priority) in self.email_addresses:
      email_address = GetField(field_name)
      if email_address:
        contact_entry.email.append(gdata.data.Email(
            address=email_address, primary=is_primary, rel=rel))

    for (field_name, rel) in self.postal_addresses:
      postal_address = GetField(field_name)      
      if postal_address:
        contact_entry.structured_postal_address.append(
          gdata.data.StructuredPostalAddress(
            formatted_address=gdata.data.FormattedAddress(text=postal_address),
            rel=rel))

    for (field_name, rel, priority) in self.phone_numbers:
      phone_number = GetField(field_name)
      if phone_number:
        contact_entry.phone_number.append(gdata.data.PhoneNumber(
            text=phone_number, rel=rel))
        
    for (field_name, rel) in self.websites:
      website = GetField(field_name)      
      if website:
        contact_entry.website.append(
          gdata.contacts.data.Website(
            href=website,
            rel=rel))

    return contact_entry

  def CreateCsvWriter(self, csv_file):
    """Creates a CSV writer the given file.

    Writes the CSV column names to the file.

    Args:
      csv_file: The file to write CSV entries to, as a writable stream.

    Returns:
      The created csv.DictWriter.
    """
    csv_writer = UnicodeDictWriter(csv_file, delimiter=',',
                                fieldnames=self.export_fields)
    csv_writer.writerow(dict(zip(self.export_fields, self.export_fields)))
    return csv_writer

  def ContactEntryToFields(self, contact_entry):
    """Converts a ContactsEntry/FeedEntry to a CSV row dictionary.

    The CSV row columns are supposed to be self.export_fields.

    Args:
      contact_entry: The gdata.contacts.data.ContactEntry instance to convert.

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
      AddField('Company', contact_entry.organization.title, 'text')
      AddField('Job Title', contact_entry.organization.title, 'text')

    AddField('Notes', contact_entry.content, 'text')       

    postal_addresses = {}
    for structured_postal_address in contact_entry.structured_postal_address:
      postal_addresses.setdefault(structured_postal_address.rel,
                                  structured_postal_address.formatted_address.text)
    for (field_name, rel) in self.postal_addresses:
      fields[field_name] = postal_addresses.get(rel, '')    

    phone_numbers = [{},{},{},{},{},{}]  # 6 priorities
    for phone_number in contact_entry.phone_number:
      i=0; # i for priority values for rel repetitions
      while phone_number.rel in phone_numbers[i]:
        i+=1
      else:
        phone_numbers[i].setdefault(phone_number.rel, phone_number.text)
    for (field_name, rel, priority) in self.primary_phone_numbers:
        fields[field_name] = phone_numbers[priority].get(rel, '')
        
    email_addresses = [{},{},{},{},{},{},{},{},{},{},
                       {},{},{},{},{},{},{},{},{},{},
                       {},{},{}] # 23 priorities
    for email in contact_entry.email:      
      i=0; # i for priority values for rel repetitions
      while i <= 10 and email.rel in email_addresses[i]:
        i+=1
      else:
        email_addresses[i].setdefault(email.rel, email.address)
    for (field_name, rel, _, priority) in self.email_addresses:
      fields[field_name] = email_addresses[priority].get(rel, '')
      
    websites = {}
    for website in contact_entry.website:
      websites.setdefault(website.rel, website.href)
    for (field_name, rel) in self.websites:
      fields[field_name] = websites.get(rel, '')
    
    return fields


def main():
  usage = """\
shared_contacts_profiles.py --admin=EMAIL [--clear] [--import=FILE [--output=FILE]]
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
          "added and updated contacts/profiles in the same format as --export")
  parser.add_option('--dry_run', action='store_true',
      help="does not authenticate and import contacts/profiles for real")
  parser.add_option('-e', '--export', default=None, metavar='FILE',
      dest='export_csv', help="exports all shared contacts/profiles of the domain as "
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

  # Construct the Contacts service and authenticate
  contacts_client = gdata.contacts.client.ContactsClient(domain=domain)
  contacts_client.client_login(email=admin_email,
                                password=admin_password,
                                source='shared_contacts_profiles',
                                account_type='HOSTED')
  contacts_manager = ContactsManager(contacts_client,domain)

  if clear:
    if dry_run:
      Log('--clear: ignored in dry mode')
    else:
      contacts_manager.DeleteAllContacts()

  if import_csv_file_name:
    Log('\n### Importing contacts/profiles CSV file: %s' % import_csv_file_name)
    contacts_manager.ImportMsOutlookCsv(import_csv_file_name, output_csv_file,
                                        dry_run=dry_run)

  if export_csv_file_name:
    if dry_run:
      Log('--export: ignored in dry mode')
    else:
      Log('### Exporting contacts/profiles to CSV file: %s' % export_csv_file_name)
      contact_entries = contacts_manager.GetAllContacts()
      profile_entries = contacts_manager.GetAllProfiles()
      contacts_manager.ExportMsOutlookCsv(contact_entries, profile_entries, export_csv_file)
      export_csv_file.close()


if __name__ == '__main__':
  main()
