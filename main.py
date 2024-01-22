""" Main file to insert data to BQ"""
import pandas as pd

def main(filename):
    """
    Main funtion to read CSV file
    """
    df = pd.read_csv(file=filename)
    return df

if __name__ == "__main__":
    FILENAME = '/table_schema/zwya_transactions.csv'
    main(filename=FILENAME)
