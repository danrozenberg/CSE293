import os
import csv
import datetime
from  itertools import islice

# noinspection PyMethodMayBeStatic
class Pis12DataParser():
    ''' Reads "PIS12" data from disk and parse it.'''

    def find_files(self, folder_path, fetch_num = 0, file_type="csv"):
        """
        A generator...
        Given a folder, returns names of files inside it.
        It returns as many as fetch_num names, which makes it useful in
        small scale tests
        :param folder: the folder to generate file names from. Does not process
            sub-folders
        :param fetch_num: if 0, we will generate names until they run out,
            otherwise, return at most fetch_num names
        :return: the names of the files inside folder.
        """
        found_files = os.listdir(folder_path)
        paths_read = 0

        #TODO: rename subfile variable
        for subfile in found_files:
            if os.path.isfile(folder_path + "/" +  subfile):
                if subfile.endswith(file_type):
                    yield folder_path + subfile
                    paths_read += 1
                    if 0 < fetch_num <= paths_read:
                        break

    def lines_reader(self, file_path, fetch_num = None):
        """
        Reads and yields a line, but does not process it.
        :param file_path:
        :param fetch_num:
        :return: an iterator. The iterator yields lists of values.
        """

        #TODO: fetch more than 1 line at once on islice call.

        # also accepts 0 as being "all the file"
        if fetch_num == 0:
            fetch_num = None

        #TODO: check if the file really is a csv.
        with open(file_path, "rb") as src:
            reader = csv.reader(src)
            _ = reader.next()  # throws header away
            iterator = islice(reader, 0, 1)

            lines_read = 0
            while True:
                yield iterator.next()
                lines_read += 1
                iterator = islice(reader, 0, 1)
                if 0 < fetch_num <= lines_read:
                    break


    # TODO: maybe put this in the interpreter instead...
    def parse_line(self, line):
        """
        Since this class deals with PIS12 data only, we can be very
            specific. Particularly, we know data read from this source will
            always have the same format. We will take advantage of that.
        :return: a dictionary with key-value pairs.
        """

        if len(line) < 67:
            raise ValueError("Unexpected format. Line is too short: \n" +
            str(line))

        #TODO: don't hardcode this, c'mon, that's hacky!
        # also, here is where we parse fields. Add as needed.
        answer = {'ANO': line[0], 'ANO_ADM': line[1], 'ANO_NASCIMENT': line[2],
                  'DIADESL': line[11], 'DT_ADMISSAO': line[18],
                  'EMP_EM_31_12': line[20], 'IDENTIFICAD': line[25],
                  'MES_ADM': line[35], 'MES_DESLIG': line[36],
                  'PIS': line[45]}

        return answer


import logging
class Pis12DataInterpreter():
    # TODO: make this faster. Don't create a new object all the time
    # TODO: don't recalculate a value if it has been calculated already
    # TODO: instead, check if previously calculated and return that instead.

    """ We need now a way to calculate several attributes, such as time working together...
     the problem is that there may be several different ways to calculate this,
        depending on when the data was generated.
     but we do know that a parsed line returns a dictionary, as seen on 'parse_line'
     technically, this shouldn't be resonsability of a parser, but...
     It SHOULD be responsability of a class that deals with PIS12.
     This is what this class is all about."""

    # TODO: explain why we don't interrupt program.
    # TODO: perhaps, have the object "update" its state, currently
        # we create a new object per line, kinda not good..
    def __init__(self, values):

        # configures log
        # TODO: maybe return log to original state afterwards...
        logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%d %b - %H:%M:%S -',
        level=logging.WARN)

        self.dict = values
        self.log_message = "Started"

    @property
    def year(self):
        """ :return: the year the entry relates to """
        return self.__simple_retrieval('ANO', 'info')

    @property
    def admission_date(self):
        # TODO: does tipo_adm change anything?

        if self.dict['DT_ADMISSAO'] <> '':
            # gotta pad the string with 0...
            try:
                date_string = self.dict['DT_ADMISSAO']

                # add missing zeroes to day of month
                if len(date_string) == 7:
                    date_string = '0' + str(date_string)
                return datetime.datetime.strptime(date_string,'%d%m%Y')
            except ValueError:
                self.log_message = "Could not parse DT_ADMISSAO for: " + str(self.dict)
                logging.warning(self.log_message)
                return -1

        elif self.dict['ANO_ADM'] == '' and self.dict['MES_ADM'].isdigit():

            if self.dict['MES_ADM'] == '0':
                # This happens in older records...for our purposes, let's
                # assume that they were hired long long ago, in 1900 or something.
                return datetime.datetime(1990, 1, 1)
            else:
                # I am assuming that in these cases, the worker was hired in the
                # current year.
                adm_month = int(self.dict['MES_ADM'])
                return datetime.datetime(self.year, adm_month, 1)

        elif self.dict['ANO_ADM'] == '' and self.dict['MES_ADM'] == '0':
            # This happens in older records...for our purposes, let's
            # assume worker was hired long ago.
            return datetime.datetime(1990, 1, 1)

        elif self.dict['ANO_ADM'] <> '' and self.dict['MES_ADM'] <> '' :
            # In this case, day dafaults to 01...
            adm_day = 1
            adm_month = int(self.dict['MES_ADM'])
            adm_year = int(self.dict['ANO_ADM'])
            return datetime.datetime(adm_year, adm_month, adm_day)
        else:
            self.log_message = "could not get admission date for: " + str(self.dict)
            logging.warning(self.log_message)
            return -1

    @property
    def demission_date(self):
        """ :return: demission date, or 0 if there was no demission.  """

        # Sometimes, we have a message in DIADESL
        if self.dict['DIADESL'] == 'NAO DESL ANO':
            return 0

        # if all empty, then not let go by company
        elif self.dict['DIADESL'] == '' and self.dict['MES_DESLIG'] == '':
            return 0

        # Another variation of 'not let go by company'
        elif self.dict['DIADESL'] == '' and self.dict['MES_DESLIG'] == '0':
            return 0

        # Another variation of 'not let go by company'
        elif self.dict['DIADESL'] == '0' and self.dict['MES_DESLIG'] == '0':
            return 0

        # MES_DESLIG (different from 0), but no DIADESL, let's assume that
        # the worker was let go at the first day of the month.
        elif self.dict['DIADESL'] == '' and self.dict['MES_DESLIG'].isdigit():
            dem_month = int(self.dict['MES_DESLIG'])
            dem_day = 1
            dem_year = self.year
            return datetime.datetime(dem_year, dem_month, dem_day)

        try:
            dem_month = int(self.dict['MES_DESLIG'])
            dem_day = int(self.dict['DIADESL'])
            dem_year = self.year

            # We have weird instances where MES_DESLIG is 2 and
            # the day is 29,30,31... This is pretty common
            # Let us assume those demissions happened in march 1st
            # We do this so that we don't have a gazzillion errors in the
            # output...
            if dem_month == 2 and dem_day > 28:
                dem_month = 3
                dem_day = 1
                self.log_message = "Weird february date in: " + str(self.dict)
                logging.info(self.log_message)

            # TODO: inconsistency in EMP_EM_31_12??
            # check inconsistent data:
            if (dem_month > 0 >= dem_day) or \
            (dem_month <= 0 < dem_day) :
                self.log_message = "Inconsistent MES_DESLIG or DIADESL in: " + str(self.dict)
                logging.warning(self.log_message)
                return -1
            else:
                return datetime.datetime(dem_year, dem_month, dem_day)

        except ValueError:
            self.log_message = "MES_DESLIG or DIADESL is invalid in: " + str(self.dict)
            logging.warning(self.log_message)
            return -1

    @property
    def time_at_employer(self):
        '''
        :return: the number of days (including weekends) that the worker
        was working for that employer.
        '''
        admission_date = self.admission_date
        demission_date = self.demission_date
        year = self.year

        if admission_date == -1 or demission_date == -1 or year == -1:
            self.log_message = "Unable to calculate time_at_employer for:" + str(self.dict)
            logging.warning(self.log_message)
            return -1

        # pretend worker starts on Jan-1st if date is from a previous year
        #   or if it has no admission date ( Isuppose that means he/shw was not
        #   hired that year....)
        if type(admission_date) == datetime.datetime and \
           admission_date.year < year:
            admission_date = datetime.datetime(year, 1, 1)
        if type(admission_date) == int and admission_date == 0:
            admission_date = datetime.datetime(year, 1, 1)


        # if we get a 0 for demission date, we suppose the worker was not
        #   fired that year, give it a Dec-31 date...
        if type(demission_date) == int and demission_date == 0:
            demission_date = datetime.datetime(year, 12, 31)

        # Finally, return...
        return (demission_date - admission_date).days

    @property
    def worker_id(self):
        return self.__simple_retrieval('PIS', 'info')

    @property
    def employer_id(self):
        return self.__simple_retrieval('IDENTIFICAD', 'info')

    def __simple_retrieval(self, field_name, alert_level='warn'):
        """
        for data that is straightforward to get.
        :return: an integer
        """
        try:
            return int(self.dict[field_name])
        except ValueError:
            # we don't want the execution to stop in this case
            # it is more useful to have the program log all lines that
            # are invalid, so that we can investigate all problematic cases,
            # after a single run. Otherwise, we will run the program once per
            # new problem that appears.

            # In any case, we want this only for new unexpected errors.
            # For example, if we know a field could fail, we should not 'warn'.
            #    we should instead 'info' it in its own property method.

            self.log_message = field_name + " is invalid in: " + str(self.dict)

            if alert_level == 'warn':
                logging.warning(self.log_message)
            else:
                logging.info(self.log_message)
            return -1
