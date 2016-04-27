import os
import csv
import datetime

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
    def __init__(self):
        self.__reset_private_variables()

    def __reset_private_variables(self):
        self.__year = None
        self.__admission_date = None
        self.__demission_date = None
        self.__time_at_employer = None
        self.__worker_id = None
        self.__employer_id = None

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
        self.__reset_private_variables()

    @property
    def year(self):
        """ :return: the year the entry relates to """
        if self.__year is None:
            self.__year = self.__simple_retrieval('ANO', 'info')
        return self.__year

    @property
    def admission_date(self):
        # TODO: check if tipo_adm change anything?

        if self.__admission_date is not None:
            return self.__admission_date

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
                self.__admission_date = datetime.datetime.strptime(date_string,'%d%m%Y')
                return self.__admission_date
            except ValueError:
                self.log_message = "Could not parse DT_ADMISSAO for: " + str(self.dict)
                logging.warning(self.log_message)
                self.__admission_date = -1
                return self.__admission_date

        elif ano_adm == '' and mes_adm.isdigit():

            if mes_adm == '0':
                # This happens in older records...for our purposes, let's
                # assume that they were hired long long ago, in 1900 or something.
                self.__admission_date = datetime.datetime(1990, 1, 1)
                return self.__admission_date
            else:
                # I am assuming that in these cases, the worker was hired in the
                # current year.
                adm_month = int(mes_adm)
                self.__admission_date = datetime.datetime(self.year, adm_month, 1)
                return self.__admission_date

        elif ano_adm == '' and mes_adm == '0':
            # This happens in older records...for our purposes, let's
            # assume worker was hired long ago.
            self.__admission_date = datetime.datetime(1990, 1, 1)
            return self.__admission_date

        elif ano_adm <> '' and mes_adm <> '' :
            # In this case, day dafaults to 01...
            adm_day = 1
            adm_month = int(mes_adm)
            adm_year = int(ano_adm)
            self.__admission_date = datetime.datetime(adm_year, adm_month, adm_day)
            return self.__admission_date
        else:
            self.log_message = "could not get admission date for: " + str(self.dict)
            logging.warning(self.log_message)
            self.__admission_date = -1
            return self.__admission_date

    @property
    def demission_date(self):
        """ :return: demission date, or 0 if there was no demission.  """

        if self.__demission_date is not None:
            return self.__demission_date

        dia_desl = self.dict['DIADESL']
        mes_deslig = self.dict['MES_DESLIG']

        # if all empty, then not let go by company
        if dia_desl == '' and mes_deslig == '':
            self.__demission_date = 0
            return self.__demission_date

        # Another variation of 'not let go by company'
        elif dia_desl == '' and mes_deslig == '0':
            self.__demission_date = 0
            return self.__demission_date

        # Another variation of 'not let go by company'
        elif dia_desl == '0' and mes_deslig == '0':
            self.__demission_date = 0
            return self.__demission_date

        # Sometimes, we have a message in DIADESL
        elif dia_desl == 'NAO DESL ANO':
            self.__demission_date = 0
            return self.__demission_date

        # MES_DESLIG (different from 0), but no DIADESL, let's assume that
        # the worker was let go at the first day of the month.
        elif dia_desl == '' and mes_deslig.isdigit():
            dem_month = int(mes_deslig)
            dem_day = 1
            dem_year = self.year
            self.__demission_date = datetime.datetime(dem_year, dem_month, dem_day)
            return self.__demission_date

        try:
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
                self.__demission_date = -1
                return self.__demission_date
            else:
                self.__demission_date = datetime.datetime(dem_year, dem_month, dem_day)
                return self.__demission_date


        except ValueError:
            self.log_message = "MES_DESLIG or DIADESL is invalid in: " + str(self.dict)
            logging.warning(self.log_message)
            self.__demission_date = -1
            return self.__demission_date

    @property
    def time_at_employer(self):
        '''
        :return: the number of days (including weekends) that the worker
        was working for that employer.
        '''

        if self.__time_at_employer is not None:
            return self.__time_at_employer

        admission_date = self.admission_date
        demission_date = self.demission_date
        year = self.year

        if admission_date == -1 or demission_date == -1 or year == -1:
            self.log_message = "Unable to calculate time_at_employer for:" + str(self.dict)
            logging.warning(self.log_message)
            self.__time_at_employer = -1
            return self.__time_at_employer

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
        self.__time_at_employer = (demission_date - admission_date).days
        return self.__time_at_employer

    @property
    def worker_id(self):
        if self.__worker_id is None:
            self.__worker_id = self.__simple_retrieval('PIS', 'info')
        return self.__worker_id

    @property
    def employer_id(self):
        if self.__employer_id is None:
            self.__employer_id = self.__simple_retrieval('IDENTIFICAD', 'info')
        return self.__employer_id

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
