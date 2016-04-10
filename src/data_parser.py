import os
import csv
from  itertools import islice

# noinspection PyMethodMayBeStatic
class Pis12DataParser():

    def __init__(self):
        self.__line = None

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

    def get_last_read_line(self):
        return  self.__line









