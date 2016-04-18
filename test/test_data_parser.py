import unittest
import logging
import datetime
import sys
sys.path.insert(0, '../src/')
from data_parser import Pis12DataParser
from data_parser import Pis12DataInterpreter

class TestPis12DataParser(unittest.TestCase):

    def test_find_files(self):
        parser = Pis12DataParser()

        # should return no files for empty folder
        files = parser.find_files("./empty_folder/")
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(0, len(found_files))

        # should return 3 csv files in test folder
        files = parser.find_files("./test_file_path_folder/")
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(3, len(found_files))
        
        # should return 3 csv files in test folder
        files = parser.find_files("./test_file_path_folder/", "csv")
        found_files = []
        expected_files = ["./test_file_path_folder/asd.csv",
                          "./test_file_path_folder/basd.csv",
                          "./test_file_path_folder/alpha.csv"]
        for f in files :
            found_files.append(f)
        self.assertEquals(3, len(found_files))
        self.assertItemsEqual(expected_files, found_files)

        
        # should return 1 txt files in test folder
        files = parser.find_files("./test_file_path_folder/", file_type="txt")
        found_files = []
        expected_files = ["./test_file_path_folder/nope.txt"]
        for f in files :
            found_files.append(f)
        self.assertEquals(1, len(found_files))
        self.assertItemsEqual(expected_files, found_files)


        # should return 2 csv files in test folder if requested for only 2
        files = parser.find_files("./test_file_path_folder/", 2)
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(2, len(found_files))

    def test_raw_lines_reader(self):
        parser = Pis12DataParser()

        # read entire file
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv")
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(30, lines_read)

        # read just a bit
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv", 22)
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(22, lines_read)

        # can't read more than what is already there...
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv", 999)
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(30, lines_read)

    def test_lines_reader(self):

        parser = Pis12DataParser()

        # read
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv")
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(30, lines_read)

    def test_parse_line(self):
        parser = Pis12DataParser()

        with self.assertRaises(ValueError) as bad_call:
            parser.parse_line([1,2,3])
        the_exception = bad_call.exception
        self.assertIn("Unexpected format", the_exception.message)

        valid_line = ['2010', '', '', '', '', '',
                      '-1', '785', '', '', '', '',
                      '', '', '', '', '', '', '',
                      '', '1', '8', '10', '', '',
                      '100', '', '', '', '', '',
                      '', '', '', '', '7', '5',
                      '', '4483', '10', '', '',
                      '', '', '', '1', '', '',
                      '', '28,10', '', '30,3',
                      '', '', '', '1', '1', '25',
                      '', '19,24', '', '1', '', '',
                      '1', '', '']
        answer = parser.parse_line(valid_line)

        # test only a few of the entries in the dictionary
        self.assertEquals('2010', answer['ANO'])
        self.assertEquals('1', answer['PIS'])
        self.assertEquals('100', answer['IDENTIFICAD'])


class TestPis12DataInterpreter(unittest.TestCase):

    """Note, KeyError should not happen in RL,
            if it does, we will not treat exception"""

    def setUp(self):
        logging.disable(logging.CRITICAL)


    def test_ano(self):
        interpreter = Pis12DataInterpreter({'ANO':1999})
        answer = interpreter.year
        self.assertEquals(1999, answer)

        # Bad value
        interpreter = Pis12DataInterpreter({'ANO':'asd'})
        answer = interpreter.year
        self.assertIn("ANO is invalid", interpreter.log_message)
        self.assertEquals(-1, answer)

        # Bad value
        interpreter = Pis12DataInterpreter({'ANO':''})
        answer = interpreter.year
        self.assertIn("ANO is invalid", interpreter.log_message)
        self.assertEquals(-1, answer)

    def test_job_start_date(self):
        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'10082015'})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(2015,8,10), answer)

        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'5041986'})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(1986,4,5), answer)

        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'05041986'})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(1986,4,5), answer)

        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'541986'})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(1986,4,5), answer)

        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'5486'})
        answer = interpreter.admission_date
        self.assertEquals(-1, answer)
        self.assertIn("Could not parse DT_ADMISSAO for: ", interpreter.log_message)

        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'28092007',
                                            'MES_ADM':'01',
                                            'ANO_ADM':'2001'})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(2007,9,28), answer)


        # with 0 as MES_ADM and ANO_ADM = ''
        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'',
                                            'MES_ADM':'0',
                                            'ANO_ADM':'',
                                            'ANO':'2002'})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(1990,1,1), answer)

        # with a number in MES_ADM and ANO_ADM = ''
        interpreter = Pis12DataInterpreter({'DT_ADMISSAO':'',
                                            'MES_ADM':'7',
                                            'ANO_ADM':'',
                                            'ANO':'2002'})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(2002,7,1), answer)


        # with no dt_admissao
        interpreter = Pis12DataInterpreter({'MES_ADM':'1',
                                            'ANO_ADM':'2001',
                                            'DT_ADMISSAO':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(2001,1,1), answer)

        interpreter = Pis12DataInterpreter({'MES_ADM':'5',
                                            'ANO_ADM':'1999',
                                            'DT_ADMISSAO':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime.datetime(1999,5,1), answer)

        interpreter = Pis12DataInterpreter({'MES_ADM':'',
                                            'ANO_ADM':'',
                                            'DT_ADMISSAO':''})
        answer = interpreter.admission_date
        self.assertEquals(-1 , answer)

    def test_job_end_date(self):
        interpreter = Pis12DataInterpreter({'DIADESL':'NAO DESL ANO'})
        answer = interpreter.demission_date
        self.assertEquals(0, answer)

        interpreter = Pis12DataInterpreter({'ANO':'1999',
                                            'DIADESL':'24',
                                            'MES_DESLIG':'12'})
        answer = interpreter.demission_date
        self.assertEquals(datetime.datetime(1999,12,24), answer)


        interpreter = Pis12DataInterpreter({'ANO':'1999',
                                            'DIADESL':'',
                                            'MES_DESLIG':'0'})
        self.assertEquals(0 , interpreter.demission_date)

        # got mes_deslig but no diadesl
        interpreter = Pis12DataInterpreter({'ANO':'2000',
                                            'DIADESL':'',
                                            'MES_DESLIG':'3'})
        self.assertEquals(datetime.datetime(2000,3,1)
                          ,interpreter.demission_date)



        interpreter = Pis12DataInterpreter({'ANO':'1999',
                                            'DIADESL':'0',
                                            'MES_DESLIG':'12'})
        answer = interpreter.demission_date
        self.assertIn("Inconsistent MES_DESLIG or DIADESL", interpreter.log_message)
        self.assertEquals(-1, answer)

        interpreter = Pis12DataInterpreter({'ANO':'1999',
                                            'DIADESL':'10',
                                            'MES_DESLIG':'-12'})
        answer = interpreter.demission_date
        self.assertIn("Inconsistent MES_DESLIG or DIADESL", interpreter.log_message)
        self.assertEquals(-1, answer)

        interpreter = Pis12DataInterpreter({'ANO':'2015',
                                            'ANO_ADM':'',
                                            'MES_ADM':'',
                                            'DT_ADMISSAO':'',
                                            'DIADESL':'',
                                            'MES_DESLIG':''})
        self.assertEquals(0, interpreter.demission_date)

    def test_worker_id(self):
        interpreter = Pis12DataInterpreter({'PIS':'131313'})
        answer = interpreter.worker_id
        self.assertEquals(131313, answer)

        interpreter = Pis12DataInterpreter({'PIS':''})
        answer = interpreter.worker_id
        self.assertIn("PIS is invalid in", interpreter.log_message)
        self.assertEquals(-1, answer)

    def test_employer_id(self):
        interpreter = Pis12DataInterpreter({'IDENTIFICAD':'888'})
        answer = interpreter.employer_id
        self.assertEquals(888, answer)

        interpreter = Pis12DataInterpreter({'IDENTIFICAD':'asd'})
        answer = interpreter.employer_id
        self.assertIn("IDENTIFICAD is invalid in", interpreter.log_message)
        self.assertEquals(-1, answer)

    def test_time_at_employer(self):

        # testing with admission date in a past year
        interpreter = Pis12DataInterpreter({'ANO':'2015',
                                            'ANO_ADM':'',
                                            'MES_ADM':'',
                                            'DT_ADMISSAO':'10081999',
                                            'DIADESL':'24',
                                            'MES_DESLIG':'12'})
        self.assertEquals(357, interpreter.time_at_employer)

        # same result, because start working on 1/1
        interpreter = Pis12DataInterpreter({'ANO':'2015',
                                            'ANO_ADM':'2015',
                                            'MES_ADM':'1',
                                            'DT_ADMISSAO':'',
                                            'DIADESL':'24',
                                            'MES_DESLIG':'12'})
        self.assertEquals(357, interpreter.time_at_employer)

        # no start date, should return invalid (that is, -1)
        interpreter = Pis12DataInterpreter({'ANO':'2015',
                                            'ANO_ADM':'',
                                            'MES_ADM':'',
                                            'DT_ADMISSAO':'',
                                            'DIADESL':'',
                                            'MES_DESLIG':''})
        self.assertEquals(-1, interpreter.time_at_employer)
        self.assertIn("Unable to calculate time_at_employer for:", interpreter.log_message)




if __name__ == "__main__":
    unittest.main()
