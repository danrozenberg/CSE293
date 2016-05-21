import os
import csv
from datetime import datetime
from time import mktime

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

        #TODO: use Pands instead because it seems that its way faster.
        # also accepts 0 as being "all the file"
        if fetch_num == 0:
            fetch_num = None

        with open(file_path, "rb") as src:
            reader = csv.reader(src)
            _ = reader.next()  # throws header away

            lines_read = 0
            while True:
                yield reader.next()
                lines_read += 1
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
                  'PIS': line[45], 'MUNICIPIO': line[38]}

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
    def __init__(self):
        self._reset_private_variables()

    def _reset_private_variables(self):
        self._year = None
        self._municipality = None
        self._admission_date = None
        self._demission_date = None
        self._admission_timestamp = None
        self._demission_timestamp = None
        self._time_at_employer = None
        self._worker_id = None
        self._employer_id = None

    def feed_line(self, values):
        """
        Feeds internal state to be ready to interpret a set of values.
        :param line: a parsed line, normally in dictionary format.
        :return: nothing
        """
        self.dict = values
        self.log_message = "Read values"

        # reset private variables
        # private variables save state so we only have to calculate
        # each property once, even if requested more than once
        # then again, we don't calculate it if we don't request it
        self._reset_private_variables()

    @property
    def year(self):
        """ :return: the year the entry relates to """
        if self._year is None:
            self._year = self._simple_retrieval('ANO', 'info')
        return self._year

    @property
    def municipality(self):
        """  :return: municipality as a string """
        if self._municipality is None:
            self._municipality = self.dict['MUNICIPIO']
        return self._municipality

    @property
    def admission_date(self):
        # TODO: check if tipo_adm change anything?

        if self._admission_date is not None:
            return self._admission_date

        dt_admissao = self.dict['DT_ADMISSAO']
        ano_adm = self.dict['ANO_ADM']
        mes_adm = self.dict['MES_ADM']

        if dt_admissao <> '':
            # gotta pad the string with 0...
            try:
                date_string = dt_admissao

                # add missing zeroes to day of month
                if len(date_string) == 7:
                    date_string = '0' + str(date_string)
                self._admission_date = datetime.strptime(date_string,'%d%m%Y')
                return self._admission_date
            except ValueError:
                self.log_message = "Could not parse DT_ADMISSAO for: " + str(self.dict)
                logging.warning(self.log_message)
                self._admission_date = -1
                return self._admission_date

        elif ano_adm == '' and mes_adm.isdigit():

            if mes_adm == '0':
                # This happens in older records...for our purposes, let's
                # assume that they were hired long long ago, in 1900 or something.
                self._admission_date = datetime(1900, 1, 1)
                return self._admission_date
            else:
                # I am assuming that in these cases, the worker was hired in the
                # current year.
                adm_month = int(mes_adm)
                self._admission_date = datetime(self.year, adm_month, 1)
                return self._admission_date

        elif ano_adm == '' and mes_adm == '0':
            # This happens in older records...for our purposes, let's
            # assume worker was hired long ago.
            self._admission_date = datetime(1900, 1, 1)
            return self._admission_date

        elif ano_adm <> '' and mes_adm <> '' :
            # In this case, day dafaults to 01...
            adm_day = 1
            adm_month = int(mes_adm)
            adm_year = int(ano_adm)
            self._admission_date = datetime(adm_year, adm_month, adm_day)
            return self._admission_date
        else:
            self.log_message = "could not get admission date for: " + str(self.dict)
            logging.warning(self.log_message)
            self._admission_date = -1
            return self._admission_date

    @property
    def demission_date(self):
        # TODO: explain the whole unix timestamp idea

        if self._demission_date is not None:
            return self._demission_date

        dia_desl = self.dict['DIADESL']
        mes_deslig = self.dict['MES_DESLIG']

        try:
            # Many ways of saying "not let go by the company"
            if (dia_desl == '' and mes_deslig == '') or \
                (dia_desl == '' and mes_deslig == '0') or \
                (dia_desl == '0' and mes_deslig == '0') or \
                (dia_desl == 'LVA' and mes_deslig == '0') or \
                (dia_desl == 'NAO DESL ANO'):
                    self._demission_date =  datetime(self.year, 12, 31)
                    return self._demission_date

            # MES_DESLIG (different from 0), but no DIADESL, let's assume that
            # the worker was let go at the first day of the month.
            elif dia_desl == '' and mes_deslig.isdigit():
                dem_month = int(mes_deslig)
                dem_day = 1
                dem_year = self.year
                self._demission_date = datetime(dem_year, dem_month, dem_day)
                return self._demission_date

            else:
                dem_month = int(mes_deslig)
                dem_day = int(dia_desl)
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
                    self._demission_date = -1
                    return self._demission_date
                else:
                    # FINALLY, we return for the normal case...
                    self._demission_date = datetime(dem_year, dem_month, dem_day)
                    return self._demission_date

        except ValueError:
            self.log_message = "MES_DESLIG or DIADESL is invalid in: " + str(self.dict)
            logging.warning(self.log_message)
            self._demission_date = -1
            return self._demission_date

    @property
    def admission_timestamp(self):
        """
        :return: admission date in unix timestamp,
        That is, seconds from Jan 1st 1970.
        """
        if self._admission_timestamp is None:
            admission_date = self.admission_date
            if admission_date == -1 :
                return -1
            else:
                # Windows cant handle mktime for dates before 1970...
                epoch = datetime(1970, 1, 1)
                difference = admission_date - epoch
                self._admission_timestamp = difference.total_seconds()

        return self._admission_timestamp

    @property
    def demission_timestamp(self):
        """
        :return: demission date in unix timestamp,
        That is, seconds from Jan 1st 1970.
        """
        if self._demission_timestamp is None:
            demission_date = self.demission_date
            if demission_date == -1:
                return  -1
            else:
                # Windows cant handle mktime for dates before 1970...
                epoch = datetime(1970, 1, 1)
                difference = demission_date - epoch
                self._demission_timestamp = difference.total_seconds()

        return self._demission_timestamp

    @property
    def time_at_employer(self):
        '''
        :return: the number of days (including weekends) that the worker
        was working for that employer.
        '''

        if self._time_at_employer is not None:
            return self._time_at_employer

        admission_date = self.admission_date
        demission_date = self.demission_date
        year = self.year

        # should fail if we have any invalid value, that is, not a
        #  datetime. invalud values are set to -1
        try:

            # for time at employer, if hired before the start of the year
            # consider only the time at employer this year
            if admission_date < datetime(year, 1, 1):
                admission_date = datetime(year, 1, 1)

            total_days = (demission_date - admission_date).days
            # we do not consider values greater than 365, this is because
            # our date sources entries represent at most a time period of
            # 1 year. So time at employer this year is capped at 365 days
            # this cap is used when the employee was neither hired nor fired
            # this 'year'
            self._time_at_employer = min(365, total_days)
            return self._time_at_employer
        except TypeError:
            self.log_message = "Unable to calculate time_at_employer for:" + str(self.dict)
            logging.warning(self.log_message)
            self._time_at_employer = -1
            return self._time_at_employer

    @property
    def worker_id(self):
        if self._worker_id is None:
            self._worker_id = self._simple_retrieval('PIS', 'info')
        return self._worker_id

    @property
    def employer_id(self):
        if self._employer_id is None:
            self._employer_id = self._simple_retrieval('IDENTIFICAD', 'info')
        return self._employer_id

    def _simple_retrieval(self, field_name, alert_level='warn'):
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

class ClassificationLoader():

    def __init__(self):
        self.dictionary = None

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

        # also accepts 0 as being "all the file"
        if fetch_num == 0:
            fetch_num = None

        with open(file_path, "rb") as src:
            reader = csv.reader(src)
            _ = reader.next()  # throws header away

            lines_read = 0
            while True:
                yield reader.next()
                lines_read += 1
                if 0 < fetch_num <= lines_read:
                    break

    def parse_line(self, line):
        if len(line) < 9:
            raise ValueError("Unexpected format. Line is too short: \n" +
            str(line))

        # also, here is where we parse fields. Add as needed.
        answer = {'FIRM_ID': line[0], 'PLANT_ID': line[1],
        'FIRST_YEAR': line[2], 'EMPLOYEE_SPINOFF': line[5],
        'DIVESTITURE': line[6], 'UNRELATED': line[7],
        'DIVERSIFY': line[8]}

        self.dictionary = answer
        return answer

    @property
    def plant_id(self):
        if self.dictionary is None:
            return None
        else:
            plant_id = self.dictionary['FIRM_ID'] + self.dictionary['PLANT_ID']
            return int(plant_id)

    @property
    def first_year(self):
        if self.dictionary is None:
            return None
        else:
            return int(self.dictionary['FIRST_YEAR'])

    @property
    def firm_type(self):
        if self.dictionary is None:
            return None
        if self.dictionary['EMPLOYEE_SPINOFF'] == '1' and \
            self.dictionary['DIVESTITURE'] == '0' and \
            self.dictionary['UNRELATED'] == '0' and \
            self.dictionary['DIVERSIFY'] == '0':
            return "EMPLOYEE_SPINOFF"

        elif self.dictionary['EMPLOYEE_SPINOFF'] == '0' and \
            self.dictionary['DIVESTITURE'] == '1' and \
            self.dictionary['UNRELATED'] == '0' and \
            self.dictionary['DIVERSIFY'] == '0':
            return 'DIVESTITURE'

        elif self.dictionary['EMPLOYEE_SPINOFF'] == '0' and \
            self.dictionary['DIVESTITURE'] == '0' and \
            self.dictionary['UNRELATED'] == '1' and \
            self.dictionary['DIVERSIFY'] == '0':
            return 'UNRELATED'

        elif self.dictionary['EMPLOYEE_SPINOFF'] == '0' and \
            self.dictionary['DIVESTITURE'] == '0' and \
            self.dictionary['UNRELATED'] == '0' and \
            self.dictionary['DIVERSIFY'] == '1':
            return 'DIVERSIFY'

        else:
            return 'UNKNOWN'




#TODO: put logging inside a method to save lines!
