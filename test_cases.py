# Databricks notebook source
options={'header':True,
         'inferschema':True,
         'delimiter':','}

def read_csv(format,options,path):
    return spark.read.format(format).options(**options).load(path)

# COMMAND ----------

spark.conf.set("spark.databricks.delta.formatCheck.enabled", "false")

# COMMAND ----------

import unittest 

class Testcase(unittest.TestCase):
    def testcount(self):
        options={'header':True,
         'inferschema':True,
         'delimiter':','}
        path='dbfs:/FileStore/emp_tables/20240105_sales_customer.csv'
        format='csv'

        actual_df=read_csv(format,options,path)
        # actual_df.display()
        df=spark.read.format('csv').load('dbfs:/FileStore/emp_tables/20240105_sales_customer.csv')
        self.assertEqual(df.display(),actual_df.display())

s = unittest.TestLoader().loadTestsFromTestCase(Testcase)
unittest.TextTestRunner(verbosity=2).run(s)

# COMMAND ----------

def reverse(s):
    return s[::-1]

# COMMAND ----------

import unittest 

class Testcase(unittest.TestCase):
    def testcount(self):
        self.assertEqual(reverse('abc'),'cba')

r=unittest.main(argv=[''],verbosity=2,exit=False)
assert r.result.wasSuccessful(),'Test failed error'

# COMMAND ----------

import unittest

class test_(unittest.TestCase):
    def testAdd(self): # test method names begin with 'test'
        self.assertEqual((1 + 2), 3)
        # self.assertEqual(0 + 1, 1)
    def testMultiply(self):
        self.assertEqual((0 * 10), 0)
        # self.assertEqual((5 * 8), 40)
    
s = unittest.TestLoader().loadTestsFromTestCase(test_)
unittest.TextTestRunner(verbosity=2).run(s)

# COMMAND ----------

import unittest

class DataFrameTestCase(unittest.TestCase):
 
    def test_schema_comparison(self):
        options={'header':True,
         'inferschema':True,
         'delimiter':','}
        format='csv'
        path='dbfs:/FileStore/emp_tables/20240105_sales_customer.csv'
        
        actual_df = read_csv(format,options,path)
        expected_df = spark.read.csv('dbfs:/FileStore/emp_tables/20240105_sales_customer.csv',header=True,inferSchema=True)
        self.assertEqual(actual_df.schema, expected_df.schema)
 
    def test_row_count_comparison(self):
        options={'header':True,
         'inferschema':True,
         'delimiter':','}
        format='csv'
        path='dbfs:/FileStore/emp_tables/20240105_sales_customer.csv'
        actual_df = read_csv(format,options,path)
        expected_df = spark.read.csv('dbfs:/FileStore/emp_tables/20240105_sales_customer.csv',header=True,inferSchema=True)
        self.assertEqual(actual_df.count(), expected_df.count())

s = unittest.TestLoader().loadTestsFromTestCase(DataFrameTestCase)
unittest.TextTestRunner(verbosity=2).run(s)

# COMMAND ----------

import unittest

class DataFrameTestCase_Pk(unittest.TestCase):
        def check_primary_key_duplicates(self, df, primary_key_columns):
        # """
        # Check if a DataFrame has any duplicates in its primary key column(s).
        
        # Parameters:
        # - df: DataFrame to check for duplicates.
        # - primary_key_columns: List of column names representing the primary key.
        
        # Returns:
        # - Boolean: True if duplicates are found, False otherwise.
        # """
            duplicates_df = df.groupBy(primary_key_columns).count().filter('count > 1')

            return duplicates_df.count() > 0

        def test_primary_key_duplicates(self):
            options={'header':True,
                    'inferschema':True,
                    'delimiter':','}
            format='csv'
            path='dbfs:/FileStore/emp_tables/20240105_sales_customer.csv'
            actual_df = read_csv(format,options,path)
            # Assuming 'Customer Id' is the primary key column
            primary_key_columns = ['Customer Id']
            has_duplicates = self.check_primary_key_duplicates(actual_df, primary_key_columns)
            self.assertFalse(has_duplicates, "Primary key has duplicates")

s = unittest.TestLoader().loadTestsFromTestCase(DataFrameTestCase_Pk)
unittest.TextTestRunner(verbosity=2).run(s)

# COMMAND ----------


