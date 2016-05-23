import unittest
import logging
from datetime import datetime
from time import mktime
import sys
sys.path.insert(0, '../src/')
from data_parser import Pis12DataParser
from data_parser import Pis12DataInterpreter
from data_parser import ClassificationLoader

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
        interpreter = Pis12DataInterpreter()

        interpreter.feed_line({'ANO':1999})
        answer = interpreter.year
        self.assertEquals(1999, answer)

        # Bad value
        interpreter.feed_line({'ANO':'asd'})
        answer = interpreter.year
        self.assertIn("ANO is invalid", interpreter.log_message)
        self.assertEquals(-1, answer)

        # Bad value
        interpreter.feed_line({'ANO':''})
        answer = interpreter.year
        self.assertIn("ANO is invalid", interpreter.log_message)
        self.assertEquals(-1, answer)

    def test_job_start_date(self):
        interpreter = Pis12DataInterpreter()

        interpreter.feed_line({'DT_ADMISSAO':'10082015',
                               'MES_ADM':'',
                               'ANO_ADM':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime(2015,8,10), answer)
        self.assertAlmostEqual(mktime(datetime(2015,8,10).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

        interpreter.feed_line({'DT_ADMISSAO':'5041986',
                               'MES_ADM':'',
                               'ANO_ADM':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime(1986,4,5), answer)
        self.assertAlmostEqual(mktime(datetime(1986,4,5).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

        interpreter.feed_line({'DT_ADMISSAO':'05041986',
                               'MES_ADM':'',
                               'ANO_ADM':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime(1986,4,5), answer)
        self.assertAlmostEqual(mktime(datetime(1986,4,5).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

        interpreter.feed_line({'DT_ADMISSAO':'541986',
                               'MES_ADM':'',
                               'ANO_ADM':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime(1986,4,5), answer)
        self.assertAlmostEqual(mktime(datetime(1986,4,5).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

        interpreter.feed_line({'DT_ADMISSAO':'5486',
                               'MES_ADM':'',
                               'ANO_ADM':''})
        answer = interpreter.admission_date
        self.assertEquals(-1, answer)
        self.assertIn("Could not parse DT_ADMISSAO for: ", interpreter.log_message)
        self.assertEquals(-1, interpreter.admission_timestamp)

        interpreter.feed_line({'DT_ADMISSAO':'28092007',
                                'MES_ADM':'01',
                                'ANO_ADM':'2001'})
        answer = interpreter.admission_date
        self.assertEquals(datetime(2007,9,28), answer)
        self.assertAlmostEqual(mktime(datetime(2007,9,28).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)


        # with 0 as MES_ADM and ANO_ADM = ''
        interpreter.feed_line({'DT_ADMISSAO':'',
                                'MES_ADM':'0',
                                'ANO_ADM':'',
                                'ANO':'2002'})
        answer = interpreter.admission_date
        self.assertEquals(datetime(1900,1,1), answer)
        self.assertAlmostEqual(mktime(datetime(1900,1,1).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

        # with a number in MES_ADM and ANO_ADM = ''
        interpreter.feed_line({'DT_ADMISSAO':'',
                                'MES_ADM':'7',
                                'ANO_ADM':'',
                                'ANO':'2002'})
        answer = interpreter.admission_date
        self.assertEquals(datetime(2002,7,1), answer)
        self.assertAlmostEqual(mktime(datetime(2002,7,1).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)


        # with no dt_admissao
        interpreter.feed_line({'MES_ADM':'1',
                                'ANO_ADM':'2001',
                                'DT_ADMISSAO':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime(2001,1,1), answer)
        self.assertAlmostEqual(mktime(datetime(2001,1,1).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

        interpreter.feed_line({'MES_ADM':'5',
                                'ANO_ADM':'1999',
                                'DT_ADMISSAO':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime(1999,5,1), answer)
        self.assertAlmostEqual(mktime(datetime(1999,5,1).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

        interpreter.feed_line({'MES_ADM':'',
                                'ANO_ADM':'',
                                'DT_ADMISSAO':''})
        answer = interpreter.admission_date
        self.assertEquals(-1 , answer)
        self.assertEquals(-1, interpreter.admission_timestamp)

        # DT_ADMISSAO with a funky format
        # should be interpreted as 3/12/2005 as in 3rd of december...
        interpreter.feed_line({'DT_ADMISSAO':'3122005',
                                'MES_ADM':'',
                                'ANO_ADM':'',
                                'ANO':''})
        answer = interpreter.admission_date
        self.assertEquals(datetime(2005,12,3), answer)
        self.assertAlmostEquals(mktime(datetime(2005,12,3).timetuple()),
                          interpreter.admission_timestamp, delta = 43200)

    def test_job_end_date(self):
        interpreter = Pis12DataInterpreter()
        interpreter.feed_line({'DIADESL':'NAO DESL ANO',
                               'MES_DESLIG':'',
                               'ANO':'2007'})
        self.assertEquals(datetime(2007,12,31), interpreter.demission_date)
        self.assertAlmostEqual(mktime(datetime(2007,12,31).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)

        interpreter.feed_line({'ANO':'1999',
                                'DIADESL':'24',
                                'MES_DESLIG':'12'})
        self.assertEquals(datetime(1999,12,24), interpreter.demission_date)
        self.assertAlmostEqual(mktime(datetime(1999,12,24).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)

        interpreter.feed_line({'ANO':'1999',
                                'DIADESL':'',
                                'MES_DESLIG':'0'})
        self.assertEquals(datetime(1999,12,31),
                          interpreter.demission_date)
        self.assertAlmostEqual(mktime(datetime(1999,12,31).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)

        # got mes_deslig but no diadesl
        interpreter.feed_line({'ANO':'2000',
                                'DIADESL':'',
                                'MES_DESLIG':'3'})
        self.assertEquals(datetime(2000,3,1)
                          ,interpreter.demission_date)
        self.assertAlmostEqual(mktime(datetime(2000,3,1).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)

        # "not fired"
        interpreter.feed_line({'ANO':'2001',
                                'DIADESL':'0',
                                'MES_DESLIG':'0'})
        self.assertEquals(datetime(2001,12,31),
                          interpreter.demission_date)
        self.assertAlmostEqual(mktime(datetime(2001,12,31).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)

        interpreter.feed_line({'ANO':'2003',
                                'DIADESL':'LVA',
                                'MES_DESLIG':'0'})
        self.assertEquals(datetime(2003,12,31),
                          interpreter.demission_date)
        self.assertAlmostEqual(mktime(datetime(2003,12,31).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)


        interpreter.feed_line({'ANO':'1999',
                                'DIADESL':'0',
                                'MES_DESLIG':'12'})
        answer = interpreter.demission_date
        self.assertIn("Inconsistent MES_DESLIG or DIADESL", interpreter.log_message)
        self.assertEquals(-1, answer)
        self.assertEquals(-1,interpreter.demission_timestamp)

        interpreter.feed_line({'ANO':'1999',
                                'DIADESL':'10',
                                'MES_DESLIG':'-12'})
        answer = interpreter.demission_date
        self.assertIn("Inconsistent MES_DESLIG or DIADESL", interpreter.log_message)
        self.assertEquals(-1, answer)
        self.assertEquals(-1,interpreter.demission_timestamp)

        interpreter.feed_line({'ANO':'2015',
                                'ANO_ADM':'',
                                'MES_ADM':'',
                                'DT_ADMISSAO':'',
                                'DIADESL':'',
                                'MES_DESLIG':''})
        self.assertEquals(datetime(2015,12,31),
                          interpreter.demission_date)
        self.assertAlmostEqual(mktime(datetime(2015,12,31).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)

        # a confusing date...put it as march 1st...
        interpreter.feed_line({'ANO':'2005',
                                'DIADESL':'29',
                                'MES_DESLIG':'2'})
        self.assertEquals(datetime(2005,3,1)
                          , interpreter.demission_date)
        self.assertIn("Weird february date in: ", interpreter.log_message)
        self.assertAlmostEqual(mktime(datetime(2005,3,1).timetuple()),
                          interpreter.demission_timestamp, delta = 43200)

    def test_worker_id(self):
        interpreter = Pis12DataInterpreter()

        interpreter.feed_line({'PIS':'131313'})
        answer = interpreter.worker_id
        self.assertEquals(131313, answer)

        interpreter.feed_line({'PIS':''})
        answer = interpreter.worker_id
        self.assertIn("PIS is invalid in", interpreter.log_message)
        self.assertEquals(-1, answer)

    def test_employer_id(self):
        interpreter = Pis12DataInterpreter()

        interpreter.feed_line({'IDENTIFICAD':'888'})
        answer = interpreter.employer_id
        self.assertEquals(888, answer)

        interpreter.feed_line({'IDENTIFICAD':'asd'})
        answer = interpreter.employer_id
        self.assertIn("IDENTIFICAD is invalid in", interpreter.log_message)
        self.assertEquals(-1, answer)

    def test_municipality(self):
        interpreter = Pis12DataInterpreter()
        interpreter.feed_line({'MUNICIPIO':'553333'})
        answer = interpreter.municipality
        self.assertEquals('553333', answer)

        interpreter.feed_line({'IDENTIFICAD':'asd',
                               'MUNICIPIO':''})
        answer = interpreter.municipality
        self.assertEquals('', answer)

    def test_time_at_employer(self):
        interpreter = Pis12DataInterpreter()

        # testing with admission date in a past year
        interpreter.feed_line({'ANO':'2015',
                                'ANO_ADM':'',
                                'MES_ADM':'',
                                'DT_ADMISSAO':'10081999',
                                'DIADESL':'24',
                                'MES_DESLIG':'12'})
        self.assertEquals(357, interpreter.time_at_employer)

        # same result, because start working on 1/1
        interpreter.feed_line({'ANO':'2015',
                                'ANO_ADM':'2015',
                                'MES_ADM':'1',
                                'DT_ADMISSAO':'',
                                'DIADESL':'24',
                                'MES_DESLIG':'12'})
        self.assertEquals(357, interpreter.time_at_employer)

        # no start date, should return invalid (that is, -1)
        interpreter.feed_line({'ANO':'2015',
                                'ANO_ADM':'',
                                'MES_ADM':'',
                                'DT_ADMISSAO':'',
                                'DIADESL':'',
                                'MES_DESLIG':''})
        self.assertEquals(-1, interpreter.time_at_employer)
        self.assertIn("Unable to calculate time_at_employer for:", interpreter.log_message)

class TestClassificationLoader(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def test_find_files(self):
        loader = ClassificationLoader()

        # should return no files for empty folder
        files = loader.find_files("./empty_folder/")
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(0, len(found_files))

        # should return 3 csv files in test folder
        files = loader.find_files("./test_file_path_folder/")
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(3, len(found_files))

        # should return 3 csv files in test folder
        files = loader.find_files("./test_file_path_folder/", "csv")
        found_files = []
        expected_files = ["./test_file_path_folder/asd.csv",
                          "./test_file_path_folder/basd.csv",
                          "./test_file_path_folder/alpha.csv"]
        for f in files :
            found_files.append(f)
        self.assertEquals(3, len(found_files))
        self.assertItemsEqual(expected_files, found_files)


        # should return 1 txt files in test folder
        files = loader.find_files("./test_file_path_folder/", file_type="txt")
        found_files = []
        expected_files = ["./test_file_path_folder/nope.txt"]
        for f in files :
            found_files.append(f)
        self.assertEquals(1, len(found_files))
        self.assertItemsEqual(expected_files, found_files)


        # should return 2 csv files in test folder if requested for only 2
        files = loader.find_files("./test_file_path_folder/", 2)
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(2, len(found_files))

    def test_lines_reader(self):
        parser = ClassificationLoader()

        # read
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv")
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(30, lines_read)

    def test_parse_line(self):
        parser = ClassificationLoader()

        with self.assertRaises(ValueError) as bad_call:
            parser.parse_line([1,2,3])
        the_exception = bad_call.exception
        self.assertIn("Unexpected format", the_exception.message)

        valid_line = ['7','56750184','1999','777','1995','0','0','1','0']
        answer = parser.parse_line(valid_line)

        # test only a few of the entries in the dictionary
        self.assertEquals('7', answer['FIRM_ID'])
        self.assertEquals('56750184', answer['CNPJ'])
        self.assertEquals('1', answer['UNRELATED'])
        self.assertEquals('0', answer['DIVERSIFY'])

    def test_truth_dictionary(self):
        parser = ClassificationLoader()

        valid_line = ['73','245245','1999','777','1995','0','0','1','0']
        parser.parse_line(valid_line)
        valid_line = ['73','879879','1999','777','1995','0','0','0','1']
        parser.parse_line(valid_line)
        valid_line = ['73','213123','1999','777','1995','0','0','0','0']
        parser.parse_line(valid_line)
        valid_line = ['73','98776','1999','777','1995','1','0','1','1']
        parser.parse_line(valid_line)

        truth_data = parser.truth_data
        self.assertEqual(4, len(truth_data))

    def test_properties(self):
        parser = ClassificationLoader()

        # should return none with no line fed...
        self.assertIsNone(parser.plant_id)

        valid_line = ['73','56750184','1999','777','1995','0','0','1','0']
        parser.parse_line(valid_line)
        self.assertEquals(56750184, parser.plant_id)
        self.assertEquals(1999, parser.first_year)
        self.assertEquals('UNRELATED', parser.entrant_type)

        valid_line = ['73','56750184','1999','777','1995','0','0','0','1']
        parser.parse_line(valid_line)
        self.assertEquals('DIVERSIFY', parser.entrant_type)

        valid_line = ['73','56750184','1999','777','1995','0','0','0','0']
        parser.parse_line(valid_line)
        self.assertEquals('UNKNOWN', parser.entrant_type)

        valid_line = ['73','56750184','1999','777','1995','1','0','1','1']
        parser.parse_line(valid_line)
        self.assertEquals('UNKNOWN', parser.entrant_type)




if __name__ == "__main__":
    unittest.main()
