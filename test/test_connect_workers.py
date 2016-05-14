import unittest
import sys
import logging
from datetime import datetime
from time import mktime
sys.path.insert(0, '../src/')
import graph_manager
import connect_workers

class WorkerConnector(unittest.TestCase):
    def setUp(self):
        logging.disable(logging.CRITICAL)

    def test_get_overlapping_days(self):
        start_1 =  mktime(datetime(2010,1,1).timetuple())
        end_1 = mktime(datetime(2015,1,1).timetuple())
        start_2 = mktime(datetime(2011,10,10).timetuple())
        end_2 = mktime(datetime(2011,10,20).timetuple())
        self.assertEquals(11, connect_workers.get_overlapping_days(start_1,
                                                                   end_1,
                                                                   start_2,
                                                                   end_2))

        # we can pass datetimes or timestamps :)
        start_2 = datetime(2010,1,1)
        end_2 = datetime(2015,1,1)
        start_1 = datetime(2011,10,10)
        end_1 = datetime(2011,10,20)
        self.assertEquals(11, connect_workers.get_overlapping_days(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_2 = mktime(datetime(2010,1,1).timetuple())
        end_2 = mktime(datetime(2010,1,1).timetuple())
        start_1 = mktime(datetime(2010,1,1).timetuple())
        end_1 = mktime(datetime(2010,1,1).timetuple())
        self.assertEquals(1, connect_workers.get_overlapping_days(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_2 = mktime(datetime(2010,1,1).timetuple())
        end_2 = mktime(datetime(2010,1,2).timetuple())
        start_1 = mktime(datetime(2010,1,2).timetuple())
        end_1 = mktime(datetime(2010,1,2).timetuple())
        self.assertEquals(1,connect_workers.get_overlapping_days(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_1 = mktime(datetime(2010,1,1).timetuple())
        end_1 = mktime(datetime(2015,1,1).timetuple())
        start_2 = mktime(datetime(2066,10,10).timetuple())
        end_2 = mktime(datetime(2077,10,20).timetuple())
        self.assertEquals(0, connect_workers.get_overlapping_days(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_1 = mktime(datetime(2010,1,1).timetuple())
        end_1 = mktime(datetime(2015,1,1).timetuple())
        start_2 = mktime(datetime(2011,10,10).timetuple())
        end_2 = mktime(datetime(2012,10,20).timetuple())
        self.assertEquals(377, connect_workers.get_overlapping_days(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

    def test_get_worker_iterator(self):
        manager = graph_manager.SnapManager()

        # add a few nodes
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node(40)
        manager.add_node(50)
        manager.add_node(60)
        manager.add_node(70)

        # make some worker nodes, others as employer nodes
        manager.add_node_attr(10, "type", "worker")
        manager.add_node_attr(20, "type", "employer")
        manager.add_node_attr(30, "type", "worker")
        manager.add_node_attr(40, "type", "employer")
        manager.add_node_attr(50, "type", "worker")
        manager.add_node_attr(60, "type", "employer")
        manager.add_node_attr(70, "type", "worker")

        # get iterator
        it = connect_workers.get_worker_iterator(manager)

        # test it
        found_ids = []
        for node in it:
            self.assertEquals("worker",
                              manager.get_node_attr(node, "type"))
            found_ids.append(node)

        self.assertListEqual([10,30,50,70], found_ids)

    def test_get_time_at_worker_attrs(self):
        manager = graph_manager.SnapManager()

        # create 2 workers, 1 employee node
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(888) # the employer

        # edge with no info
        worker_edge = 10
        coworker_edge = 20
        manager.add_edge(1,888, worker_edge)
        manager.add_edge(2,888, coworker_edge)

        # Nothing so far
        worker_edge_attrs =  manager.get_edge_attrs(worker_edge)
        coworker_edge_attrs =  manager.get_edge_attrs(coworker_edge)
        time_together = connect_workers.get_time_together(worker_edge_attrs,
                                                          coworker_edge_attrs)
        self.assertEquals(0, time_together)

        #lets say they worked together for 19 days in 2013
        manager.add_edge_attr(worker_edge,
                              "2013_admission_date",
                              mktime(datetime(2013,1,1).timetuple()))
        manager.add_edge_attr(worker_edge,
                              "2013_demission_date",
                              mktime(datetime(2013,12,31).timetuple()))

        manager.add_edge_attr(coworker_edge,
                              "2013_admission_date",
                              mktime(datetime(2013,4,1).timetuple()))
        manager.add_edge_attr(coworker_edge,
                              "2013_demission_date",
                              mktime(datetime(2013,4,20).timetuple()))
        worker_edge_attrs =  manager.get_edge_attrs(worker_edge)
        coworker_edge_attrs =  manager.get_edge_attrs(coworker_edge)
        time_together = connect_workers.get_time_together(worker_edge_attrs,
                                                          coworker_edge_attrs)
        self.assertEquals(19, time_together)


        #lets say they didn't work together in 2014
        manager.add_edge_attr(worker_edge,
                              "2014_admission_date",
                              mktime(datetime(2013,1,1).timetuple()))
        manager.add_edge_attr(worker_edge,
                              "2014_demission_date",
                              mktime(datetime(2013,12,31).timetuple()))

        self.assertEquals(19, time_together)

        #worked more together again in 2015
        manager.add_edge_attr(worker_edge,
                              "2015_admission_date",
                              mktime(datetime(2015,1,1).timetuple()))
        manager.add_edge_attr(worker_edge,
                              "2015_demission_date",
                              mktime(datetime(2015,3,31).timetuple()))

        manager.add_edge_attr(coworker_edge,
                              "2015_admission_date",
                              mktime(datetime(2015,2,1).timetuple()))
        manager.add_edge_attr(coworker_edge,
                              "2015_demission_date",
                              mktime(datetime(2015,12,31).timetuple()))
        worker_edge_attrs =  manager.get_edge_attrs(worker_edge)
        coworker_edge_attrs =  manager.get_edge_attrs(coworker_edge)
        time_together = connect_workers.get_time_together(worker_edge_attrs,
                                                          coworker_edge_attrs)
        self.assertEquals(77, time_together)

    def test_connect_workers_with_min_days(self):
        manager = graph_manager.SnapManager()
        self.create_affiliation_graph(manager)

        new_graph = graph_manager.SnapManager()
        connector = connect_workers.WorkerConnector()
        connector.min_days_together = 1
        connector.connect_workers(manager, new_graph)

        # Graph should have 9 nodes only
        self.assertEqual(9, new_graph.get_node_count())

        # All edges that should exist
        self.assertTrue(new_graph.is_edge_between(1,2))
        self.assertTrue(new_graph.is_edge_between(1,3))
        self.assertTrue(new_graph.is_edge_between(9,2))
        self.assertTrue(new_graph.is_edge_between(9,4))
        self.assertTrue(new_graph.is_edge_between(5,4))
        self.assertTrue(new_graph.is_edge_between(5,6))
        self.assertTrue(new_graph.is_edge_between(5,7))

        # some edges that should not exist
        self.assertFalse(new_graph.is_edge_between(7,7))
        self.assertFalse(new_graph.is_edge_between(9,8))
        self.assertFalse(new_graph.is_edge_between(9,7))
        self.assertFalse(new_graph.is_edge_between(1,7))
        self.assertFalse(new_graph.is_edge_between(1,5))

        # worked together, but not at the same time
        self.assertFalse(new_graph.is_edge_between(2,3))
        self.assertFalse(new_graph.is_edge_between(4,6))
        self.assertFalse(new_graph.is_edge_between(7,4))
        self.assertFalse(new_graph.is_edge_between(9,5))
        self.assertFalse(new_graph.is_edge_between(1,9))
        self.assertFalse(new_graph.is_edge_between(9,1))

    def test_connect_workers_no_min_days(self):
        manager = graph_manager.SnapManager()
        self.create_affiliation_graph(manager)

        new_graph = graph_manager.SnapManager()
        connector = connect_workers.WorkerConnector()
        connector.connect_workers(manager,new_graph)

        # Graph should have 9 nodes only
        self.assertEqual(9, new_graph.get_node_count())

        # All edges that should exist
        self.assertTrue(new_graph.is_edge_between(1,2))
        self.assertTrue(new_graph.is_edge_between(1,3))
        self.assertTrue(new_graph.is_edge_between(2,3))
        self.assertTrue(new_graph.is_edge_between(9,1))
        self.assertTrue(new_graph.is_edge_between(9,2))
        self.assertTrue(new_graph.is_edge_between(9,3))
        self.assertTrue(new_graph.is_edge_between(9,4))
        self.assertTrue(new_graph.is_edge_between(9,5))
        self.assertTrue(new_graph.is_edge_between(4,7))
        self.assertTrue(new_graph.is_edge_between(4,6))
        self.assertTrue(new_graph.is_edge_between(4,5))
        self.assertTrue(new_graph.is_edge_between(5,6))
        self.assertTrue(new_graph.is_edge_between(5,7))
        self.assertTrue(new_graph.is_edge_between(6,9))

        # some edges that should not exist
        self.assertFalse(new_graph.is_edge_between(3,7))
        self.assertFalse(new_graph.is_edge_between(7,7))
        self.assertFalse(new_graph.is_edge_between(9,8))
        self.assertFalse(new_graph.is_edge_between(9,7))
        self.assertFalse(new_graph.is_edge_between(1,7))
        self.assertFalse(new_graph.is_edge_between(1,5))

    def test_from_timestamp(self):
        expected = datetime(1980,10,15)
        timestamp = mktime(expected.timetuple())
        actual = connect_workers.from_timestamp(timestamp)
        self.assertAlmostEqual((expected - actual).seconds, 0, delta=61200)

        expected = datetime(1920,1,15)
        timestamp = mktime(expected.timetuple())
        actual = connect_workers.from_timestamp(timestamp)
        self.assertAlmostEqual((expected - actual).seconds, 0, delta=61200)

        expected = datetime(2040,2,22)
        timestamp = mktime(expected.timetuple())
        actual = connect_workers.from_timestamp(timestamp)
        self.assertAlmostEqual((expected - actual).seconds, 0, delta=61200)

    def test_should_skip(self):
        new_graph = graph_manager.SnapManager()
        new_graph.add_node(1)
        new_graph.add_node(2)
        new_graph.add_node(3)
        new_graph.add_node(4)

        # # don't connect worker with himself
        self.assertTrue(connect_workers.should_skip(1,1, new_graph))

        # # don't skip unconnected workers
        self.assertFalse(connect_workers.should_skip(1,2, new_graph))
        self.assertFalse(connect_workers.should_skip(1,3, new_graph))

        # do skip connected workers

        new_graph.add_edge(1,3)
        new_graph.add_edge(1,4)
        self.assertTrue(connect_workers.should_skip(1,3, new_graph))
        self.assertTrue(connect_workers.should_skip(4,1, new_graph))
        self.assertFalse(connect_workers.should_skip(1,2, new_graph))

    def create_affiliation_graph(self, manager):
        # 9 workers
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        manager.add_node(5)
        manager.add_node(6)
        manager.add_node(7)
        manager.add_node(8)
        manager.add_node(9)
        manager.add_node_attr(1, "type", "worker")
        manager.add_node_attr(2, "type", "worker")
        manager.add_node_attr(3, "type", "worker")
        manager.add_node_attr(4, "type", "worker")
        manager.add_node_attr(5, "type", "worker")
        manager.add_node_attr(6, "type", "worker")
        manager.add_node_attr(7, "type", "worker")
        manager.add_node_attr(8, "type", "worker")
        manager.add_node_attr(9, "type", "worker")

        # 3 plants
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node_attr(10, "type", "employer")
        manager.add_node_attr(20, "type", "employer")
        manager.add_node_attr(30, "type", "employer")

        # connect them
        manager.add_edge(1, 10, 100)
        manager.add_edge(2, 10, 200)
        manager.add_edge(3, 10, 300)
        manager.add_edge(9, 10, 400)

        manager.add_edge(4, 20, 500)
        manager.add_edge(5, 20, 600)
        manager.add_edge(6, 20, 700)
        manager.add_edge(9, 20, 800)

        manager.add_edge(4, 30, 900)
        manager.add_edge(5, 30, 1000)
        manager.add_edge(7, 30, 1100)


        # 1 worked with 3 and 2
        manager.add_edge_attr(100, "1999_admission_date",
                              mktime(datetime(1999,1,1).timetuple()))
        manager.add_edge_attr(100, "1999_demission_date",
                              mktime(datetime(1999,12,31).timetuple()))
        manager.add_edge_attr(100, "2000_admission_date",
                              mktime(datetime(2000,1,1).timetuple()))
        manager.add_edge_attr(100, "2000_demission_date",
                              mktime(datetime(2000,12,31).timetuple()))
        manager.add_edge_attr(100, "2001_admission_date",
                              mktime(datetime(2001,1,1).timetuple()))
        manager.add_edge_attr(100, "2001_demission_date",
                              mktime(datetime(2001,12,31).timetuple()))

        manager.add_edge_attr(200, "1999_admission_date",
                              mktime(datetime(1999,5,5).timetuple()))
        manager.add_edge_attr(200, "1999_demission_date",
                              mktime(datetime(1999,12,31).timetuple()))

        manager.add_edge_attr(300, "2001_admission_date",
                              mktime(datetime(2001,6,6).timetuple()))
        manager.add_edge_attr(300, "2001_demission_date",
                              mktime(datetime(2001,12,31).timetuple()))

        # 2 worked with 9
        manager.add_edge_attr(200, "2002_admission_date",
                              mktime(datetime(2002,5,5).timetuple()))
        manager.add_edge_attr(200, "2002_demission_date",
                              mktime(datetime(2002,12,31).timetuple()))
        manager.add_edge_attr(400, "2002_admission_date",
                              mktime(datetime(2002,1,1).timetuple()))
        manager.add_edge_attr(400, "2002_demission_date",
                              mktime(datetime(2002,6,1).timetuple()))

        # 9 worked with 4
        manager.add_edge_attr(800, "2003_admission_date",
                              mktime(datetime(2003,1,1).timetuple()))
        manager.add_edge_attr(800, "2003_demission_date",
                              mktime(datetime(2003,6,1).timetuple()))
        manager.add_edge_attr(800, "2004_admission_date",
                              mktime(datetime(2004,1,1).timetuple()))
        manager.add_edge_attr(800, "2004_demission_date",
                              mktime(datetime(2004,6,1).timetuple()))

        manager.add_edge_attr(500, "2004_admission_date",
                              mktime(datetime(2004,1,1).timetuple()))
        manager.add_edge_attr(500, "2004_demission_date",
                              mktime(datetime(2004,10,31).timetuple()))


        # 4 worked with 5
        manager.add_edge_attr(600, "2004_admission_date",
                              mktime(datetime(2004,7,1).timetuple()))
        manager.add_edge_attr(600, "2004_demission_date",
                              mktime(datetime(2004,12,31).timetuple()))

        # 5 worked with 6
        manager.add_edge_attr(700, "2004_admission_date",
                              mktime(datetime(2004,11,1).timetuple()))
        manager.add_edge_attr(700, "2004_demission_date",
                              mktime(datetime(2004,12,25).timetuple()))

        # 5 worked with 7
        manager.add_edge_attr(1000, "2005_admission_date",
                              mktime(datetime(2005,1,1).timetuple()))
        manager.add_edge_attr(1000, "2005_demission_date",
                              mktime(datetime(2005,7,1).timetuple()))

        manager.add_edge_attr(1100, "2005_admission_date",
                              mktime(datetime(2005,1,1).timetuple()))
        manager.add_edge_attr(1100, "2005_demission_date",
                              mktime(datetime(2005,6,1).timetuple()))

        # try to trick the algorithm
        # 4 will work with 5 again, but in a different company
        manager.add_edge_attr(900, "2005_admission_date",
                              mktime(datetime(2005,7,1).timetuple()))
        manager.add_edge_attr(900, "2005_demission_date",
                              mktime(datetime(2005,7,1).timetuple()))

if __name__ == "__main__":
    unittest.main()
