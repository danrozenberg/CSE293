import os
import csv
import datetime
from  itertools import islice

# noinspection PyMethodMayBeStatic
class Pis12DataParser():
    ''' Reads "PIS12" data from disk and parse it.'''

    def __init__(self):
        pass

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
        answer = {'ANO': line[0], 'ANO_ADM': line[1], 'ANO_NASCIMENT': line[2],
                  'CAUSAFAST1': line[3], 'CAUSAFAST2': line[4],
                  'CAUSAFAST3': line[5], 'CAUS_DESL': line[6],
                  'CBOGRP': line[7], 'CEI_VINC': line[8], 'CLASCNAE20': line[9],
                  'CLAS_CNAE_95': line[10], 'DIADESL': line[11],
                  'DIAFIMAF1': line[12], 'DIAFIMAF2': line[13],
                  'DIAFIMAF3': line[14], 'DIAINIAF1': line[15],
                  'DIAINIAF2': line[16], 'DIAINIAF3': line[17],
                  'DT_ADMISSAO': line[18], 'DT_NASCIMENT': line[19],
                  'EMP_EM_31_12': line[20], 'FX_ETARIA': line[21],
                  'GRAU_INSTR': line[22], 'HORAS_CONTR': line[23],
                  'IDADE': line[24], 'IDENTIFICAD': line[25],
                  'IND_ALVARA': line[26], 'IND_PAT': line[27],
                  'IND_SIMPLES': line[28], 'MESFIMAF1': line[29],
                  'MESFIMAF2': line[30], 'MESFIMAF3': line[31],
                  'MESINIAF1': line[32], 'MESINIAF2': line[33],
                  'MESINIAF3': line[34], 'MES_ADM': line[35],
                  'MES_DESLIG': line[36], 'MES_NASCIMENT': line[37],
                  'MUNICIPIO': line[38], 'NACIONALIDAD': line[39],
                  'NAT_ESTB': line[40], 'NAT_JURID': line[41],
                  'NUME_CTPS': line[42], 'OCUPACAO02': line[43],
                  'OCUPACAO94': line[44], 'PIS': line[45],
                  'PORT_DEFIC': line[46], 'QTDIASAFAS': line[47],
                  'RACA_COR': line[48], 'REM_DEZEMBRO': line[49],
                  'REM_DEZ__R__': line[50], 'REM_MEDIA': line[51],
                  'REM_MED__R__': line[52], 'SAL_CONTR': line[53],
                  'SBCLAS20': line[54], 'SEXO': line[55],
                  'SIT_VINCULO': line[56], 'SUBS_IBGE': line[57],
                  'TAMESTAB': line[58], 'TEMP_EMPR': line[59],
                  'TIPO_ADM': line[60], 'TIPO_ESTBL': line[61],
                  'TIPO_ESTB_ID': line[62], 'TIPO_SAL': line[63],
                  'TIPO_VINC': line[64], 'TPDEFIC': line[65],
                  'ULT_REM ': line[66]}

        return answer


import logging
class Pis12DataInterpreter():
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
            #
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
