import unittest
import mapper
import reducer


class unit_test_cases(unittest.TestCase):
    """
    Test the functions from mapper and reducer
    """

    # Test csvDictRows from mapper
    def test_csvDictRows(self):
        input = ['1000000001,Smith,James,AMBIEN,100',
                 '1000000002,Garcia,Maria,AMBIEN,200',
                 '1000000003,Johnson,James,CHLORPROMAZINE,1000',
                 '1000000004,Rodriguez,Maria,CHLORPROMAZINE,2000',
                 '1000000005,Smith,David,BENZTROPINE MESYLATE,1500']
        expected = ['AMBIEN\tSmith James\t100', 'AMBIEN\tGarcia Maria\t200',
                    'CHLORPROMAZINE\tJohnson James\t1000', 'CHLORPROMAZINE\tRodriguez Maria\t2000',
                    'BENZTROPINE MESYLATE\tSmith David\t1500']

        result = []
        for output in mapper.csvDictRows(input):
            result.append(output)

        print('result: ', result)
        self.assertEqual(expected, result)

    # Test get_reducer_feeds from reducer
    def test_get_reducer_feeds(self):

        input = ['AMBIEN\tSmith James\t100', 'AMBIEN\tGarcia Maria\t200']

        expected = [('AMBIEN', 'Smith James', 100), ('AMBIEN', 'Garcia Maria', 200)]

        result = []

        for output in reducer.get_reducer_feeds(input):
            result.append(output)

        print('result: ', result)
        self.assertEqual(expected, result)

    # Test reducer from reducer
    def test_reducer(self):

        input = ['AMBIEN\tSmith James\t100', 'AMBIEN\tGarcia Maria\t200']

        expected = ['AMBIEN\t2\t300']

        result = []

        for output in reducer.reducer(input):
            result.append(output)

        print('result: ', result)
        self.assertEqual(expected, result)


if __name__ == '__main__':
    unittest.main()
