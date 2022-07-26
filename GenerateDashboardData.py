import processing_lib.dashboard_data_lib as dd
import os

if __name__=="__main__":

    os.makedirs('dashboard_data', exist_ok=True)
    df = dd.get_final_dataframe(2)
    df.to_csv('dashboard_data\country_tokens.csv')
